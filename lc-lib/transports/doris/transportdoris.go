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
	columnDefs   map[string]string // column name -> type mapping
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
	// Initialize column definitions with hard-coded defaults
	t.columnDefs = map[string]string{
		"@timestamp":             "DATETIME",
		"message":                "STRING",
		"host":                   "STRING",
		"path":                   "STRING",
		"type":                   "STRING",
		"tags":                   "ARRAY<STRING>",
		t.config.RestJSONColumn:  "JSON",
	}

	// Add additional columns from configuration
	for colName, colType := range t.config.additionalColumnDefs {
		t.columnDefs[colName] = colType
	}

	// Check if table exists using DESCRIBE
	describeSQL := fmt.Sprintf("DESCRIBE `%s`.`%s`", t.config.Database, t.config.Table)
	httpRequest, err := t.createRequest(t.ctx, "POST", addr, "/api/query/default_cluster/"+t.config.Database, strings.NewReader(describeSQL))
	if err != nil {
		return err
	}

	httpRequest.Header.Add("Content-Type", "text/plain")

	httpResponse, err := t.getClient(addr).Do(httpRequest)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()

	if httpResponse.StatusCode == 200 {
		// Table exists - check columns
		return t.validateAndUpdateColumns(addr, body)
	}

	// Table doesn't exist - create it
	return t.createTable(addr)
}

// validateAndUpdateColumns validates existing columns and adds missing ones
func (t *transportDoris) validateAndUpdateColumns(addr *addresspool.Address, describeResult []byte) error {
	// Parse DESCRIBE result to get existing columns
	// This is a simplified parse - in production you'd want more robust parsing
	existingCols := make(map[string]string)
	lines := strings.Split(string(describeResult), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			colName := strings.Trim(fields[0], "`")
			colType := fields[1]
			existingCols[colName] = colType
		}
	}

	// Check for columns with wrong types
	for colName, expectedType := range t.columnDefs {
		if existingType, exists := existingCols[colName]; exists {
			// Normalize type comparison
			if !strings.EqualFold(normalizeType(existingType), normalizeType(expectedType)) {
				return fmt.Errorf("column '%s' has type '%s' but expected '%s' - manual schema fix needed", colName, existingType, expectedType)
			}
		}
	}

	// Add missing columns
	var missingCols []string
	for colName := range t.columnDefs {
		if _, exists := existingCols[colName]; !exists {
			missingCols = append(missingCols, colName)
		}
	}

	if len(missingCols) == 0 {
		log.Infof("[T %s] Doris table %s.%s schema is valid", addr.Desc(), t.config.Database, t.config.Table)
		return nil
	}

	// Add missing columns
	for _, colName := range missingCols {
		colType := t.columnDefs[colName]
		alterSQL := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s", t.config.Database, t.config.Table, colName, colType)
		
		httpRequest, err := t.createRequest(t.ctx, "POST", addr, "/api/query/default_cluster/"+t.config.Database, strings.NewReader(alterSQL))
		if err != nil {
			return err
		}

		httpRequest.Header.Add("Content-Type", "text/plain")

		httpResponse, err := t.getClient(addr).Do(httpRequest)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(httpResponse.Body)
		httpResponse.Body.Close()

		if httpResponse.StatusCode != 200 {
			return fmt.Errorf("failed to add column '%s': %s [Body: %s]", colName, httpResponse.Status, body)
		}

		log.Infof("[T %s] Added column '%s' to table %s.%s", addr.Desc(), colName, t.config.Database, t.config.Table)
	}

	return nil
}

// createTable creates a new table with proper schema and partitioning
func (t *transportDoris) createTable(addr *addresspool.Address) error {
	var columnDefs []string
	
	// Always include @timestamp first as it's the partition key
	columnDefs = append(columnDefs, fmt.Sprintf("`%s` %s", "@timestamp", t.columnDefs["@timestamp"]))
	
	// Add other columns in a consistent order
	for colName, colType := range t.columnDefs {
		if colName != "@timestamp" {
			columnDefs = append(columnDefs, fmt.Sprintf("`%s` %s", colName, colType))
		}
	}

	// Build partition definition
	partitionClause := "PARTITION BY RANGE(`@timestamp`) () "

	// Build properties including replication and partition retention
	properties := []string{
		`"replication_num" = "1"`,
		fmt.Sprintf(`"dynamic_partition.enable" = "true"`),
		fmt.Sprintf(`"dynamic_partition.time_unit" = "DAY"`),
		fmt.Sprintf(`"dynamic_partition.start" = "-%d"`, t.config.PartitionRetentionDays),
		fmt.Sprintf(`"dynamic_partition.end" = "3"`),
		fmt.Sprintf(`"dynamic_partition.prefix" = "p"`),
		fmt.Sprintf(`"dynamic_partition.buckets" = "10"`),
	}

	createSQL := fmt.Sprintf(
		"CREATE TABLE `%s`.`%s` (%s) DUPLICATE KEY(`@timestamp`) %sDISTRIBUTED BY HASH(`@timestamp`) BUCKETS 10 PROPERTIES (%s)",
		t.config.Database,
		t.config.Table,
		strings.Join(columnDefs, ", "),
		partitionClause,
		strings.Join(properties, ", "),
	)

	httpRequest, err := t.createRequest(t.ctx, "POST", addr, "/api/query/default_cluster/"+t.config.Database, strings.NewReader(createSQL))
	if err != nil {
		return err
	}

	httpRequest.Header.Add("Content-Type", "text/plain")

	httpResponse, err := t.getClient(addr).Do(httpRequest)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()

	if httpResponse.StatusCode != 200 {
		return fmt.Errorf("failed to create table: %s [Body: %s]", httpResponse.Status, body)
	}

	log.Infof("[T %s] Created Doris table %s.%s with %d-day retention", addr.Desc(), t.config.Database, t.config.Table, t.config.PartitionRetentionDays)
	return nil
}

// normalizeType normalizes a Doris type for comparison
func normalizeType(t string) string {
	return strings.ToUpper(strings.TrimSpace(t))
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

			request := newStreamLoadRequest(t.columnDefs, t.config.RestJSONColumn, payload.events)
			eventCount := uint32(len(payload.events))

			// Retry until successful or shutdown
			for {
				// Pool Next() is not race-safe
				t.poolMutex.Lock()
				addr, err := t.poolEntry.Next()
				t.poolMutex.Unlock()

				if err == nil {
					err = t.performStreamLoad(addr, id, request)
				}

				if err == nil {
					// Success - acknowledge all events (Doris stream load is all-or-nothing)
					select {
					case <-t.ctx.Done():
						// Forced failure
						return
					case t.eventChan <- transports.NewAckEvent(t.ctx, payload.nonce, eventCount):
					}
					break
				}

				// Log error and retry
				log.Errorf("[T %s]{%d} Doris stream load failed: %s", addr.Desc(), id, err)
				
				if t.retryWait(backoff) {
					// Shutdown requested during retry
					return
				}
			}
		}
	}
}

// performStreamLoad performs a stream load request to the Doris server
func (t *transportDoris) performStreamLoad(addr *addresspool.Address, id int, request *streamLoadRequest) error {
	url := fmt.Sprintf("/api/%s/%s/_stream_load", t.config.Database, t.config.Table)
	eventCount := request.EventCount()
	log.Debugf("[T %s]{%d} Performing Doris stream load of %d events to %s", addr.Desc(), id, eventCount, url)

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

	response, err := newStreamLoadResponse(body)
	if err != nil {
		return fmt.Errorf("response failed to parse: %s [Body: %s]", err, body)
	}

	if response.Status != "Success" && response.Status != "Publish Timeout" {
		return fmt.Errorf("stream load failed with status: %s [Message: %s]", response.Status, response.Message)
	}

	log.Debugf("[T %s]{%d} Doris stream load complete (loaded %d; filtered %d)", addr.Desc(), id, response.NumberLoadedRows, response.NumberFilteredRows)

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
