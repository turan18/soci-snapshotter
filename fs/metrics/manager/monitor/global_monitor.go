/*
   Copyright The Soci Snapshotter Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package monitor

import (
	"context"
	"time"

	cm "github.com/awslabs/soci-snapshotter/fs/metrics/common"
	"github.com/opencontainers/go-digest"
)

type globalMonitor struct {
	fuseFailureSignal chan struct{}
}

// NewGlobalMonitor returns a new global monitor. A global monitor encapsulates
// Prometheus metric operations at a global level.
func NewGlobalMonitor() Monitor {
	return &globalMonitor{
		fuseFailureSignal: make(chan struct{}),
	}
}

func (gb *globalMonitor) Inc(metric string) {
	// Note: Currently, only used to increment FUSE mount failure metric.
	// This is supposed to be at a layer monitor level, but the image
	// ref and layer digest is not directly exposed in snapshot.go.
	cm.IncOperationCount(metric, digest.Digest(""))
}

func (gb *globalMonitor) Add(metric string, v int64) {
	// Note: Currently, only used to increment background fetch work queue size.
	// This is supposed to be at an image monitor level, but the image
	// ref is not exposed in the background fetcher.
	cm.AddImageOperationCount(metric, digest.Digest(""), int32(v))
}

func (gb *globalMonitor) Measure(metric string, t time.Time, precision Precision) {

}

// Report notifies the listener that a FUSE failure occurred.
// We wrap the send in a select block with a default case to ensure
// the thread does not block if no receiver is available.
func (gb *globalMonitor) Report(metric string) {
	select {
	case gb.fuseFailureSignal <- struct{}{}:
	default:
		return
	}
}

// Listen infinitely listens for any FUSE failure. If one occurs,
// it increments the FuseFailure state metrics and sleeps for a
// time duration.
func (gb *globalMonitor) Listen(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-gb.fuseFailureSignal:
			cm.IncOperationCount(cm.FuseFailureState, digest.Digest(""))
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Minute):
			}
		}
	}
}
