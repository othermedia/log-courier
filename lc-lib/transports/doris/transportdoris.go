/*
 * Copyright 2012-2020 Jason Woods and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package doris

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/driskell/log-courier/lc-lib/addresspool"
	"github.com/driskell/log-courier/lc-lib/core"
	"github.com/driskell/log-courier/lc-lib/event"
	"github.com/driskell/log-courier/lc-lib/transports"
)

var (
	// ErrInvalidState occurs when a send cannot happen because the connection has closed
	ErrInvalidState = errors.New("invalid connection state")
)

// payload contains nonce and events information
type payload struct {
	nonce  *string
	events []*event.Event
}

type clientCacheItem struct {
	client  *http.Client
	expires time.Time
}

// transportDoris implements a transport that sends over the Doris HTTP stream load protocol
type transportDoris struct {
	// Constructor
	ctx          context.Context
	shutdownFunc context.CancelFunc
	config       *TransportDorisFactory
	netConfig    *transports.Config
	poolEntry    *addresspool.PoolEntry
	clientCache  map[string]*clientCacheItem
	eventChan    chan<- transports.Event

	// Internal
	payloadChan  chan *payload
	payloadMutex sync.Mutex
	poolMutex    sync.Mutex
	wait         sync.WaitGroup
	columnNames  []string
}

// Factory returns the associated factory
func (t *transportDoris) Factory() transports.TransportFactory {
	return t.config
}

// startController starts the controller
func (t *transportDoris) startController() {
	go t.controllerRoutine()
}

// controllerRoutine is the master routine which handles submission
func (t *transportDoris) controllerRoutine() {
	defer func() {
		// Wait for all routines to close and close all connections
		t.wait.Wait()
		for _, cacheItem := range t.clientCache {
			cacheItem.client.CloseIdleConnections()
		}
		t.eventChan <- transports.NewStatusEvent(t.ctx, transports.Finished, nil)
	}()

	if t.setupAssociation() {
		// Shutdown was requested
		return
	}

	// Setup payload chan with max write count of pending payloads
	t.payloadMutex.Lock()
	t.payloadChan = make(chan *payload, t.netConfig.MaxPendingPayloads)
	t.payloadMutex.Unlock()

	t.eventChan <- transports.NewStatusEvent(t.ctx, transports.Started, nil)

	// Start secondary http routines
	for i := 1; i < t.config.Routines; i++ {
		t.wait.Add(1)
		go t.httpRoutine(i)
	}

	// Become the main http routine
	t.wait.Add(1)
	t.httpRoutine(0)

	// Ensure all resources for the cancel are cleaned up
	t.shutdownFunc()
}

// setupAssociation ensures table and columns exist
func (t *transportDoris) setupAssociation() bool {
	backoffName := fmt.Sprintf("[T %s] Setup Retry", t.poolEntry.Server)
	backoff := core.NewExpBackoff(backoffName, t.config.Retry, t.config.RetryMax)

	for {
		addr, err := t.poolEntry.Next()
		if err != nil {
			log.Errorf("[T %s] Failed to resolve Doris node address: %s", addr.Desc(), err)
		} else if err := t.ensureTableExists(addr); err != nil {
			log.Errorf("[T %s] Failed to ensure Doris table exists: %s", addr.Desc(), err)
		} else {
			return false
		}

		if t.retryWait(backoff) {
			break
		}
	}

	// Shutdown
	return true
}

// ensureTableExists checks if the table exists and creates it with necessary columns
func (t *transportDoris) ensureTableExists(addr *addresspool.Address) error {
	// Define the column names we'll use
	// These are common fields from log-courier events
	if len(t.config.Columns) > 0 {
		t.columnNames = t.config.Columns
	} else {
		// Default columns based on common event fields
		t.columnNames = []string{
			"@timestamp",
			"message",
			"host",
			"path",
			"type",
			"tags",
			t.config.RestJSONColumn,
		}
	}

	// Check if table exists by attempting a SELECT
	checkSQL := fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1", t.config.Database, t.config.Table)
	httpRequest, err := t.createRequest(t.ctx, "POST", addr, "/api/query/default_cluster/"+t.config.Database, strings.NewReader(checkSQL))
	if err != nil {
		return err
	}

	httpRequest.Header.Add("Content-Type", "text/plain")

	httpResponse, err := t.getClient(addr).Do(httpRequest)
	if err != nil {
		return err
	}
	defer func() {
		bufio.NewReader(httpResponse.Body).WriteTo(io.Discard)
		httpResponse.Body.Close()
	}()

	// If table exists, we're done
	if httpResponse.StatusCode == 200 {
		log.Infof("[T %s] Doris table %s.%s exists", addr.Desc(), t.config.Database, t.config.Table)
		return nil
	}

	// Table doesn't exist - create it
	// Note: In production, table should be pre-created with proper schema
	// This is a basic creation for convenience
	var columnDefs []string
	for _, col := range t.columnNames {
		if col == "@timestamp" {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` DATETIME", col))
		} else if col == t.config.RestJSONColumn {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` JSON", col))
		} else if col == "tags" {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` ARRAY<STRING>", col))
		} else {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` STRING", col))
		}
	}

	createSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) DUPLICATE KEY(`@timestamp`) DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 10 PROPERTIES (\"replication_num\" = \"1\")",
		t.config.Database,
		t.config.Table,
		strings.Join(columnDefs, ", "),
	)

	httpRequest, err = t.createRequest(t.ctx, "POST", addr, "/api/query/default_cluster/"+t.config.Database, strings.NewReader(createSQL))
	if err != nil {
		return err
	}

	httpRequest.Header.Add("Content-Type", "text/plain")

	httpResponse, err = t.getClient(addr).Do(httpRequest)
	if err != nil {
		return err
	}
	defer func() {
		bufio.NewReader(httpResponse.Body).WriteTo(io.Discard)
		httpResponse.Body.Close()
	}()

	if httpResponse.StatusCode != 200 {
		body, _ := io.ReadAll(httpResponse.Body)
		return fmt.Errorf("failed to create table: %s [Body: %s]", httpResponse.Status, body)
	}

	log.Infof("[T %s] Created Doris table %s.%s", addr.Desc(), t.config.Database, t.config.Table)
	return nil
}

// httpRoutine performs stream load requests to Doris
func (t *transportDoris) httpRoutine(id int) {
	defer func() {
		t.wait.Done()
	}()

	backoffName := fmt.Sprintf("%s:%d Retry", t.poolEntry.Server, id)
	backoff := core.NewExpBackoff(backoffName, t.config.Retry, t.config.RetryMax)

	for {
		select {
		case <-t.ctx.Done():
			// Forced failure
			return
		case payload := <-t.payloadChan:
			if payload == nil {
				// Graceful shutdown
				log.Infof("[T %s]{%d} Doris routine stopped gracefully", t.poolEntry.Server, id)
				return
			}

			lastAckSequence := uint32(0)
			request := newStreamLoadRequest(t.columnNames, t.config.RestJSONColumn, payload.events)

			for {
				// Pool Next() is not race-safe
				t.poolMutex.Lock()
				addr, err := t.poolEntry.Next()
				t.poolMutex.Unlock()
				if err == nil {
					err = t.performStreamLoad(addr, id, request)
				}
				if err != nil {
					log.Errorf("[T %s]{%d} Doris stream load failed: %s", addr.Desc(), id, err)
				}

				if request.AckSequence() != lastAckSequence {
					lastAckSequence = request.AckSequence()

					select {
					case <-t.ctx.Done():
						// Forced failure
						return
					case t.eventChan <- transports.NewAckEvent(t.ctx, payload.nonce, lastAckSequence):
					}
				}

				if request.Remaining() == 0 {
					break
				}

				if t.retryWait(backoff) {
					break
				}
			}
		}
	}
}

// performStreamLoad performs a stream load request to the Doris server
func (t *transportDoris) performStreamLoad(addr *addresspool.Address, id int, request *streamLoadRequest) error {
	// Store what's already created so we can calculate what this specific request created
	created := request.Created()

	url := fmt.Sprintf("/api/%s/%s/_stream_load", t.config.Database, t.config.Table)
	log.Debugf("[T %s]{%d} Performing Doris stream load of %d events to %s", addr.Desc(), id, request.Remaining(), url)

	request.Reset()
	bodyBuffer := new(bytes.Buffer)
	zlibWriter := gzip.NewWriter(bodyBuffer)
	if _, err := io.Copy(zlibWriter, request); err != nil {
		return err
	}
	if err := zlibWriter.Close(); err != nil {
		return err
	}

	httpRequest, err := t.createRequest(t.ctx, "PUT", addr, url, bodyBuffer)
	if err != nil {
		return err
	}

	httpRequest.Header.Add("Content-Length", fmt.Sprintf("%d", bodyBuffer.Len()))
	httpRequest.Header.Add("Content-Type", "application/json")
	httpRequest.Header.Add("Content-Encoding", "gzip")
	httpRequest.Header.Add("format", "json")
	httpRequest.Header.Add("strip_outer_array", "true")
	
	// Add any custom load properties
	for key, value := range t.config.LoadProperties {
		httpRequest.Header.Add(key, value)
	}

	httpResponse, err := t.getClient(addr).Do(httpRequest)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()

	if httpResponse.StatusCode != 200 {
		return fmt.Errorf("unexpected status: %s [Body: %s]", httpResponse.Status, body)
	}

	response, err := newStreamLoadResponse(body, request)
	if err != nil {
		return fmt.Errorf("response failed to parse: %s [Body: %s]", err, body)
	}

	if response.Status != "Success" && response.Status != "Publish Timeout" {
		return fmt.Errorf("stream load failed with status: %s [Message: %s]", response.Status, response.Message)
	}

	if request.Remaining() == 0 {
		log.Debugf("[T %s]{%d} Doris stream load complete (created %d; filtered %d)", addr.Desc(), id, request.Created()-created, response.NumberFilteredRows)
	} else {
		log.Warningf("[T %s]{%d} Doris stream load partially complete (created %d; filtered %d; retrying %d)", addr.Desc(), id, request.Created()-created, response.NumberFilteredRows, request.Remaining())
	}

	return nil
}

// retryWait waits the backoff timeout before attempting to retry
// It also monitors for shutdown whilst waiting
func (t *transportDoris) retryWait(backoff *core.ExpBackoff) bool {
	now := time.Now()
	reconnectDue := now.Add(backoff.Trigger())

	select {
	case <-t.ctx.Done():
		// Shutdown request
		return true
	case <-time.After(reconnectDue.Sub(now)):
	}

	return false
}

// SendEvents sends events to the transport - only valid after Started transport event received
func (t *transportDoris) SendEvents(nonce string, events []*event.Event) error {
	// Are we ready?
	t.payloadMutex.Lock()
	defer t.payloadMutex.Unlock()
	if t.payloadChan == nil {
		return ErrInvalidState
	}
	t.payloadChan <- &payload{&nonce, events}
	return nil
}

// Ping the remote server - not implemented for HTTP since we close connections after each send
// Immediately respond with a pong
func (t *transportDoris) Ping() error {
	go func() {
		log.Debugf("[T %s] Responding with pong", t.poolEntry.Server)
		select {
		case <-t.ctx.Done():
			// Forced failure
			return
		case t.eventChan <- transports.NewPongEvent(t.ctx):
		}
	}()
	return nil
}

// Fail the transport
func (t *transportDoris) Fail() {
	t.shutdownFunc()
}

// Shutdown the transport - only valid after Started transport event received
func (t *transportDoris) Shutdown() {
	t.payloadMutex.Lock()
	defer t.payloadMutex.Unlock()
	if t.payloadChan == nil {
		// No connection active so just fail
		t.shutdownFunc()
	} else {
		// Trigger graceful shutdown
		close(t.payloadChan)
	}
}

// createRequest creates a new http.Request and adds default headers
func (t *transportDoris) createRequest(ctx context.Context, method string, addr *addresspool.Address, url string, body io.Reader) (*http.Request, error) {
	var scheme string
	if t.config.transport == TransportDorisHTTPS {
		scheme = "https"
	} else {
		scheme = "http"
	}

	request, err := http.NewRequestWithContext(ctx, method, fmt.Sprintf("%s://%s%s", scheme, addr.Addr().String(), url), body)
	if err != nil {
		return nil, err
	}

	if t.config.Username != "" && t.config.Password != "" {
		request.SetBasicAuth(t.config.Username, t.config.Password)
	}

	return request, nil
}

// getClient returns a http.Client for the given server
func (t *transportDoris) getClient(addr *addresspool.Address) *http.Client {
	t.poolMutex.Lock()
	defer t.poolMutex.Unlock()

	now := time.Now()
	expires := time.Now().Add(time.Second * 300)
	cacheItem, ok := t.clientCache[addr.Host()]
	if ok {
		cacheItem.expires = expires
		return cacheItem.client
	}

	for key, cacheItem := range t.clientCache {
		if cacheItem.expires.Before(now) {
			cacheItem.client.CloseIdleConnections()
			delete(t.clientCache, key)
		}
	}

	certPool := x509.NewCertPool()
	for _, cert := range t.config.CaList {
		certPool.AddCert(cert)
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout: t.netConfig.Timeout,
			TLSClientConfig: &tls.Config{
				RootCAs:    certPool,
				ServerName: addr.Host(),
				MinVersion: t.config.MinTLSVersion,
				MaxVersion: t.config.MaxTLSVersion,
			},
		},
		Timeout: t.netConfig.Timeout,
	}

	t.clientCache[addr.Host()] = &clientCacheItem{client, expires}
	return client
}
