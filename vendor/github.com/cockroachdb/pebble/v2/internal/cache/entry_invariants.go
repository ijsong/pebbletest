// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
//go:build (invariants && !race) || (tracing && !race)
// +build invariants,!race tracing,!race

package cache

import (
	"fmt"
	"os"

	"github.com/cockroachdb/pebble/v2/internal/invariants"
)

// When the "invariants" or "tracing" build tags are enabled, we need to
// allocate entries using the Go allocator so entry.val properly maintains a
// reference to the Value.
const entriesGoAllocated = true

func entryAllocNew() *entry {
	e := &entry{}
	// Note: this is a no-op if invariants and tracing are disabled or race is
	// enabled.
	invariants.SetFinalizer(e, func(obj interface{}) {
		e := obj.(*entry)
		if *e != (entry{}) {
			fmt.Fprintf(os.Stderr, "%p: entry was not freed", e)
			os.Exit(1)
		}
	})
	return e
}

func entryAllocFree(e *entry) {
	*e = entry{}
}
