// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"bytes"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// GossipUpdater process system config gossip updates for the SQL layer.
type GossipUpdater struct {
	// System Config and mutex.
	systemConfig   config.SystemConfig
	systemConfigMu sync.RWMutex
}

// updateSystemConfig is called whenever the system config gossip entry is updated.
func (g *GossipUpdater) updateSystemConfig(cfg config.SystemConfig) {
	g.systemConfigMu.Lock()
	defer g.systemConfigMu.Unlock()
	g.systemConfig = cfg
}

// getSystemConfig returns a pointer to the latest system config.
func (g *GossipUpdater) getSystemConfig() config.SystemConfig {
	g.systemConfigMu.RLock()
	defer g.systemConfigMu.RUnlock()
	return g.systemConfig
}

var (
	disableSyncSchemaChangeExec   = false
	asyncSchemaChangeExecInterval = 60 * time.Second
	asyncSchemaChangeExecDelay    = 360 * time.Second
)

// TestDisableSyncSchemaChangeExec is used in tests to
// disable the synchronous execution of schema changes,
// so that the asynchronous schema changer can run the
// schema changes.
func TestDisableSyncSchemaChangeExec() func() {
	disableSyncSchemaChangeExec = true
	asyncSchemaChangeExecInterval = 20 * time.Millisecond
	asyncSchemaChangeExecDelay = 20 * time.Millisecond
	return func() {
		disableSyncSchemaChangeExec = false
		asyncSchemaChangeExecInterval = 60 * time.Second
		asyncSchemaChangeExecDelay = 360 * time.Second
	}
}

// Start starts a goroutine that refreshes the lease manager
// leases for tables received in the latest system configuration via gossip.
func (g *GossipUpdater) Start(s *stop.Stopper, db *client.DB, gossip *gossip.Gossip, leaseMgr *LeaseManager) {
	s.RunWorker(func() {
		descKeyPrefix := keys.MakeTablePrefix(uint32(DescriptorTable.ID))
		gossipUpdateC := gossip.RegisterSystemConfigChannel()
		ticker := time.NewTicker(asyncSchemaChangeExecInterval)
		var schemaChangers []SchemaChanger

		for {
			select {
			case <-gossipUpdateC:
				cfg := *gossip.GetSystemConfig()
				g.updateSystemConfig(cfg)

				// Read all tables and their versions
				if log.V(2) {
					log.Info("received a new config %v", cfg)
				}
				schemaChanger := SchemaChanger{nodeID: roachpb.NodeID(leaseMgr.nodeID), db: *db, leaseMgr: leaseMgr}
				schemaChangers = nil
				// Loop through the configuration to find all the tables.
				for _, kv := range cfg.Values {
					if !bytes.HasPrefix(kv.Key, descKeyPrefix) {
						continue
					}
					// Attempt to unmarshal config into a table/database descriptor.
					var descriptor Descriptor
					if err := kv.Value.GetProto(&descriptor); err != nil {
						log.Warningf("%s: unable to unmarshal descriptor %v", kv.Key, kv.Value)
						continue
					}
					switch union := descriptor.Union.(type) {
					case *Descriptor_Table:
						table := union.Table
						if err := table.Validate(); err != nil {
							log.Errorf("%s: received invalid table descriptor: %v", kv.Key, table)
							continue
						}
						if log.V(2) {
							log.Infof("%s: refreshing lease table: %d, version: %d",
								kv.Key, table.ID, table.Version)
						}
						// Try to refresh the table lease to one >= this version.
						if err := leaseMgr.refreshLease(db, table.ID, table.Version); err != nil {
							log.Warningf("%s: %v", kv.Key, err)
						}

						// Keep track of outstanding schema changes.
						// If all schema change commands always set UpVersion, why
						// check for the presence of mutations?
						// a schema change execution might fail soon after
						// unsetting UpVersion, and we still want to process
						// outstanding mutations.
						if table.UpVersion || len(table.Mutations) > 0 {
							// Only track the first schema change. We depend on
							// gossip to renotify us when a schema change has been
							// completed.
							schemaChanger.tableID = table.ID
							if len(table.Mutations) == 0 {
								schemaChanger.mutationID = invalidMutationID
							} else {
								schemaChanger.mutationID = table.Mutations[0].MutationID
							}
							schemaChanger.cfg = cfg
							// The same schema change gets recreated with a new
							// time everytime it is seen through gossip. This can result
							// in a schema change getting starved from being executed
							// if a ton of schema changes are occurring in parallel
							// resulting in a continuous stream of gossip notifications
							// updating the creation time.
							// But that's not a realistic outcome. After the dust settles,
							// the schema change will get a fixed creation time and will
							// eventually execute.
							schemaChanger.creationTime = time.Now()
							schemaChangers = append(schemaChangers, schemaChanger)
						}

					case *Descriptor_Database:
						// Ignore.
					}
				}

			case <-ticker.C:
				for i, sc := range schemaChangers {
					if time.Since(sc.creationTime) > asyncSchemaChangeExecDelay {
						// Run schema changer in a separate goroutine because it can
						// wait on leases getting refreshed above through this
						// goroutine. Each time the ticker fires we might attempt
						// to run the same schema change in another goroutine, but that's
						// not a big deal because only one goroutine is likely to hold
						// the lease.
						s.RunAsyncTask(func() {
							// Try to run one schema change.
							if err := sc.exec(); err != nil && err != errExistingSchemaChangeLease {
								log.Info(err)
							}
						})
						// Only attempt to run one schema change. Place schema change
						// at the end of the queue.
						schemaChangers = append(schemaChangers[:i], schemaChangers[i+1:]...)
						schemaChangers = append(schemaChangers, sc)
						break
					}
				}

			case <-s.ShouldStop():
				return
			}
		}
	})
}
