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
	"sync/atomic"
	"time"

	"github.com/awslabs/soci-snapshotter/fs/layer/fuse"
	cm "github.com/awslabs/soci-snapshotter/fs/metrics/common"
	"github.com/containerd/containerd/log"
	"github.com/opencontainers/go-digest"
)

// WithWaitPeriod sets the wait period for the image monitor.
func WithWaitPeriod(waitP time.Duration) ImageMonitorOpt {
	return func(im *imageMonitor) {
		im.waitPeriod = waitP
	}
}

type ImageMonitorOpt func(*imageMonitor)

type imageMonitor struct {
	imageDigest digest.Digest
	opCounter   *fuseOperationCounter
	waitPeriod  time.Duration
}

// NewImageMonitor returns a new image monitor. An image monitor encapsulates
// Prometheus metric operations at an image level.
func NewImageMonitor(imageDigest digest.Digest, opts ...ImageMonitorOpt) Monitor {
	imgMonitor := &imageMonitor{
		imageDigest: imageDigest,
	}
	for _, o := range opts {
		o(imgMonitor)
	}
	f := &fuseOperationCounter{
		opCounts: make(map[string]*int32),
	}
	for _, m := range fuse.OpsList {
		f.opCounts[m] = new(int32)
	}
	imgMonitor.opCounter = f
	return imgMonitor
}

func (im *imageMonitor) Inc(metric string) {
	// In this case "metric" is the name of a fuse operation (eg: file.Read or node.Open).
	im.opCounter.inc(metric)
}

func (im *imageMonitor) Add(metric string, v int64) {
	cm.AddImageOperationCount(metric, im.imageDigest, int32(v))
}

// Unimplemented
func (im *imageMonitor) Measure(metric string, t time.Time, precision Precision) {

}

// Unimplemented
func (im *imageMonitor) Report(metric string) {

}

func (im *imageMonitor) Listen(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(im.waitPeriod):
		for op, opCount := range im.opCounter.opCounts {
			// We want both an aggregated metric (e.g. p90) and an image specific metric so that we can compare
			// how a specific image is behaving to a larger dataset. When the image cardinality is small,
			// we can just include the image digest as a label on the metric itself, however, when the cardinality
			// is large, this can be very expensive. Here we give consumers options by emitting both logs and
			// metrics. A low cardinality use case can rely on metrics. A high cardinality use case can
			// aggregate the metrics across all images, but still get the per-image info via logs.
			count := atomic.LoadInt32(opCount)
			im.Add(op, int64(count))
			log.G(ctx).Infof("fuse operation count for image %s: %s = %d", im.imageDigest, op, count)
		}
	}
}

// fuseOperationCounter collects number of invocations of the various FUSE
// implementations and emits them as metrics.
type fuseOperationCounter struct {
	opCounts map[string]*int32
}

// Inc atomically increase the count of FUSE operation op.
// Noop if op is not in FuseOpsList.
func (f *fuseOperationCounter) inc(op string) {
	opCount, ok := f.opCounts[op]
	if !ok {
		return
	}
	atomic.AddInt32(opCount, 1)
}
