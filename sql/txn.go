// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

// BeginTransaction starts a new transaction.
func (p *planner) BeginTransaction(n *parser.BeginTransaction) (planNode, error) {
	if !p.txn.exists() {
		return nil, util.Errorf("the server should have already created a transaction")
	}
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

// CommitTransaction commits a transaction.
func (p *planner) CommitTransaction(n *parser.CommitTransaction) (planNode, error) {
	err := p.txn.Commit()
	// Reset transaction.
	p.resetTxn()
	return &valuesNode{}, err
}

// RollbackTransaction rolls back a transaction.
func (p *planner) RollbackTransaction(n *parser.RollbackTransaction) (planNode, error) {
	err := p.txn.Rollback()
	// Reset transaction.
	p.resetTxn()
	return &valuesNode{}, err
}

// SetTransaction sets a transaction's isolation level
func (p *planner) SetTransaction(n *parser.SetTransaction) (planNode, error) {
	if err := p.setIsolationLevel(n.Isolation); err != nil {
		return nil, err
	}
	return &valuesNode{}, nil
}

func (p *planner) setIsolationLevel(level parser.IsolationLevel) error {
	switch level {
	case parser.UnspecifiedIsolation:
		return nil
	case parser.SnapshotIsolation:
		return p.txn.SetIsolation(roachpb.SNAPSHOT)
	case parser.SerializableIsolation:
		return p.txn.SetIsolation(roachpb.SERIALIZABLE)
	default:
		return util.Errorf("unknown isolation level: %s", level)
	}
}

type batchedTxn struct {
	txn   *client.Txn
	batch *client.Batch
}

func (bt *batchedTxn) exists() bool {
	return bt.txn != nil
}

func (bt *batchedTxn) set(txn *client.Txn) {
	bt.txn = txn
}

func (bt *batchedTxn) clear() {
	bt.txn = nil
}

func (bt *batchedTxn) Status() roachpb.TransactionStatus {
	return bt.txn.Proto.Status
}

func (bt *batchedTxn) SetIsolation(isolation roachpb.IsolationType) error {
	return bt.txn.SetIsolation(isolation)
}

func (bt *batchedTxn) Isolation() roachpb.IsolationType {
	return bt.txn.Proto.Isolation
}

func (bt *batchedTxn) SetSystemDBTrigger() {
	bt.txn.SetSystemDBTrigger()
}

func (bt *batchedTxn) SystemDBTrigger() bool {
	return bt.txn.SystemDBTrigger()
}

func (bt *batchedTxn) Get(key interface{}) (client.KeyValue, error) {
	if err := bt.flushBatch(); err != nil {
		return client.KeyValue{}, err
	}
	return bt.txn.Get(key)
}

func (bt *batchedTxn) GetProto(key interface{}, msg proto.Message) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.GetProto(key, msg)
}

func (bt *batchedTxn) Put(key, value interface{}) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.Put(key, value)
}

func (bt *batchedTxn) CPut(key, value, expValue interface{}) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.CPut(key, value, expValue)
}

func (bt *batchedTxn) Inc(key interface{}, value int64) (client.KeyValue, error) {
	if err := bt.flushBatch(); err != nil {
		return client.KeyValue{}, err
	}
	return bt.txn.Inc(key, value)
}

func (bt *batchedTxn) Scan(begin, end interface{}, maxRows int64) ([]client.KeyValue, error) {
	if err := bt.flushBatch(); err != nil {
		return nil, err
	}
	return bt.txn.Scan(begin, end, maxRows)
}

func (bt *batchedTxn) ReverseScan(begin, end interface{}, maxRows int64) ([]client.KeyValue, error) {
	if err := bt.flushBatch(); err != nil {
		return nil, err
	}
	return bt.txn.ReverseScan(begin, end, maxRows)
}

func (bt *batchedTxn) Del(keys ...interface{}) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.Del(keys...)
}

func (bt *batchedTxn) DelRange(begin, end interface{}) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.DelRange(begin, end)
}

func (bt *batchedTxn) Run(b *client.Batch) error {
	if err := bt.flushBatch(); err != nil {
		return err
	}
	return bt.txn.Run(b)
}

func (bt *batchedTxn) Cleanup(err error) {
	bt.batch = nil
	bt.txn.Cleanup(err)
}

func (bt *batchedTxn) Commit() error {
	if bt.batch != nil {
		b := bt.batch
		bt.batch = nil
		return bt.txn.CommitInBatch(b)
	}
	return bt.txn.CommitNoCleanup()
}

func (bt *batchedTxn) Rollback() error {
	bt.batch = nil
	return bt.txn.Rollback()
}

func (bt *batchedTxn) getBatch() *client.Batch {
	if bt.batch == nil {
		bt.batch = bt.txn.NewBatch()
	}
	return bt.batch
}

func (bt *batchedTxn) flushBatch() error {
	if bt.batch == nil {
		return nil
	}
	b := bt.batch
	bt.batch = nil
	return bt.txn.Run(b)
}
