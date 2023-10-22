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

	"github.com/awslabs/soci-snapshotter/fs/layer/fuse"
	"github.com/opencontainers/go-digest"
)

var fuseOpFailureMetrics = map[string]string{
	fuse.OpGetattr:         cm.FuseNodeGetattrFailureCount,
	fuse.OpListxattr:       cm.FuseNodeListxattrFailureCount,
	fuse.OpLookup:          cm.FuseNodeLookupFailureCount,
	fuse.OpOpen:            cm.FuseNodeOpenFailureCount,
	fuse.OpReaddir:         cm.FuseNodeReaddirFailureCount,
	fuse.OpFileRead:        cm.FuseFileReadFailureCount,
	fuse.OpFileGetattr:     cm.FuseFileGetattrFailureCount,
	fuse.OpWhiteoutGetattr: cm.FuseWhiteoutGetattrFailureCount,
}

type layerMonitor struct {
	layerDigest digest.Digest
}

// NewLayerMonitor returns a new layer monitor. A layer monitor encapsulates
// Prometheus metric operations at a layer level.
func NewLayerMonitor(layerDigest digest.Digest) Monitor {
	return &layerMonitor{layerDigest: layerDigest}
}

func (lm *layerMonitor) Inc(metric string) {
	cm.IncOperationCount(metric, lm.layerDigest)

}

func (lm *layerMonitor) Add(metric string, v int64) {
	cm.AddBytesCount(metric, lm.layerDigest, v)
}

func (lm *layerMonitor) Measure(metric string, t time.Time, precision Precision) {
	if precision == Milli {
		cm.MeasureLatencyInMilliseconds(metric, lm.layerDigest, t)
	}
	cm.MeasureLatencyInMicroseconds(metric, lm.layerDigest, t)
}

func (lm *layerMonitor) Report(fuseOp string) {
	metricLabel, ok := fuseOpFailureMetrics[fuseOp]
	if !ok {
		metricLabel = cm.FuseUnknownFailureCount
	}
	cm.IncOperationCount(metricLabel, lm.layerDigest)
}

// Unimplemented
func (lm *layerMonitor) Listen(ctx context.Context) {

}
