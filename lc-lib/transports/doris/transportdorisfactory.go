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
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/driskell/log-courier/lc-lib/addresspool"
	"github.com/driskell/log-courier/lc-lib/config"
	"github.com/driskell/log-courier/lc-lib/transports"
)

const (
	defaultRoutines       int           = 4
	defaultRetry          time.Duration = 0 * time.Second
	defaultRetryMax       time.Duration = 300 * time.Second
	defaultDatabase       string        = "default"
	defaultRestJSONColumn string        = "rest"
)

var (
	// TransportDoris is the transport name for Doris HTTP
	TransportDoris = "doris"
	// TransportDorisHTTPS is the transport name for Doris HTTPS
	TransportDorisHTTPS = "doris-https"
)

// TransportDorisFactory holds the configuration from the configuration file
// It allows creation of TransportDoris instances that use this configuration
type TransportDorisFactory struct {
	// Constructor
	config    *config.Config
	transport string

	// Configuration
	Database       string            `config:"database"`
	Table          string            `config:"table"`
	Columns        []string          `config:"columns"`
	RestJSONColumn string            `config:"rest json column"`
	Password       string            `config:"password"`
	Retry          time.Duration     `config:"retry backoff"`
	RetryMax       time.Duration     `config:"retry backoff max"`
	Routines       int               `config:"routines"`
	Username       string            `config:"username"`
	LoadProperties map[string]string `config:"load properties"`

	*transports.ClientTlsConfiguration `config:",embed"`
}

// NewTransportDorisFactory create a new TransportDorisFactory from the provided
// configuration data, reporting back any configuration errors it discovers
func NewTransportDorisFactory(p *config.Parser, configPath string, unUsed map[string]interface{}, name string) (transports.TransportFactory, error) {
	ret := &TransportDorisFactory{
		config:    p.Config(),
		transport: name,
	}
	if err := p.Populate(ret, unUsed, configPath, true); err != nil {
		return nil, err
	}
	return ret, nil
}

// Validate the configuration
func (f *TransportDorisFactory) Validate(p *config.Parser, configPath string) (err error) {
	if f.Routines < 1 {
		return fmt.Errorf("%sroutines cannot be less than 1", configPath)
	}
	if f.Routines > 32 {
		return fmt.Errorf("%sroutines cannot be more than 32", configPath)
	}

	if f.Database == "" {
		return fmt.Errorf("%sdatabase is required", configPath)
	}

	if f.Table == "" {
		return fmt.Errorf("%stable is required", configPath)
	}

	if f.RestJSONColumn == "" {
		return fmt.Errorf("%srest json column is required", configPath)
	}

	// Columns are optional - if not specified, we'll use defaults
	// based on common event fields

	return f.ClientTlsConfiguration.TlsValidate(f.transport == TransportDorisHTTPS, p, configPath)
}

// Defaults sets the default configuration values
func (f *TransportDorisFactory) Defaults() {
	f.Routines = defaultRoutines
	f.Retry = defaultRetry
	f.RetryMax = defaultRetryMax
	f.Database = defaultDatabase
	f.RestJSONColumn = defaultRestJSONColumn
	f.LoadProperties = make(map[string]string)
}

// NewTransport returns a new Transport interface using the settings from the
// TransportDorisFactory.
func (f *TransportDorisFactory) NewTransport(ctx context.Context, poolEntry *addresspool.PoolEntry, eventChan chan<- transports.Event) transports.Transport {
	ctx, shutdownFunc := context.WithCancel(ctx)

	ret := &transportDoris{
		ctx:          ctx,
		shutdownFunc: shutdownFunc,
		config:       f,
		netConfig:    transports.FetchConfig(f.config),
		poolEntry:    poolEntry,
		eventChan:    eventChan,
		clientCache:  make(map[string]*clientCacheItem),
	}

	ret.startController()
	return ret
}

// ShouldRestart returns true if the transport needs to be restarted in order
// for the new configuration to apply
func (t *TransportDorisFactory) ShouldRestart(newConfig transports.TransportFactory) bool {
	newConfigImpl := newConfig.(*TransportDorisFactory)
	if newConfigImpl.Database != t.Database {
		return true
	}
	if newConfigImpl.Table != t.Table {
		return true
	}
	if !reflect.DeepEqual(newConfigImpl.Columns, t.Columns) {
		return true
	}
	if newConfigImpl.RestJSONColumn != t.RestJSONColumn {
		return true
	}
	if newConfigImpl.Password != t.Password {
		return true
	}
	if newConfigImpl.Retry != t.Retry {
		return true
	}
	if newConfigImpl.RetryMax != t.RetryMax {
		return true
	}
	if newConfigImpl.Routines != t.Routines {
		return true
	}
	if newConfigImpl.Username != t.Username {
		return true
	}
	if !reflect.DeepEqual(newConfigImpl.LoadProperties, t.LoadProperties) {
		return true
	}

	return t.ClientTlsConfiguration.HasChanged(newConfigImpl.ClientTlsConfiguration)
}

// Register the transports
func init() {
	transports.RegisterTransport(TransportDoris, NewTransportDorisFactory)
	transports.RegisterTransport(TransportDorisHTTPS, NewTransportDorisFactory)
}
