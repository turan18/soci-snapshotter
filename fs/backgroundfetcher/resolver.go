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

package backgroundfetcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	commonmetrics "github.com/awslabs/soci-snapshotter/fs/metrics/common"
	"github.com/awslabs/soci-snapshotter/fs/metrics/manager/monitor"
	sm "github.com/awslabs/soci-snapshotter/fs/span-manager"
	"github.com/awslabs/soci-snapshotter/ztoc/compression"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

type Resolver interface {
	// Resolve fetches and caches the next span. Returns true if there is still more data to be fetched.
	// Returns false otherwise.
	Resolve(context.Context) (bool, error)

	// Closes the resolver.
	Close() error

	// Checks whether the resolver is closed or not.
	Closed() bool
}

type base struct {
	*sm.SpanManager
	layerDigest digest.Digest
	lm          monitor.Monitor
	closed      bool
	closedMu    sync.Mutex
	// timestamp when background fetch for the layer starts
	start time.Time
}

func (b *base) Close() error {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()
	b.closed = true
	return nil
}

func (b *base) Closed() bool {
	b.closedMu.Lock()
	defer b.closedMu.Unlock()
	return b.closed
}

// A sequentialLayerResolver background fetches spans sequentially, starting from span 0.
type sequentialLayerResolver struct {
	*base
	nextSpanFetchID compression.SpanID
}

func NewSequentialResolver(layerDigest digest.Digest, spanManager *sm.SpanManager, lm monitor.Monitor) Resolver {
	return &sequentialLayerResolver{
		base: &base{
			SpanManager: spanManager,
			layerDigest: layerDigest,
			lm:          lm,
		},
	}
}

func (lr *sequentialLayerResolver) Resolve(ctx context.Context) (bool, error) {
	log.G(ctx).WithFields(logrus.Fields{
		"layer":  lr.layerDigest,
		"spanId": lr.nextSpanFetchID,
	}).Debug("fetching span")

	if lr.nextSpanFetchID == 0 {
		lr.base.start = time.Now()
	}
	err := lr.FetchSingleSpan(lr.nextSpanFetchID)
	if err == nil {
		lr.lm.Inc(commonmetrics.BackgroundSpanFetchCount)
		lr.nextSpanFetchID++
		return true, nil
	}
	if errors.Is(err, sm.ErrExceedMaxSpan) {
		lr.lm.Measure(commonmetrics.BackgroundFetch, lr.base.start, monitor.Milli)
		return false, nil
	}
	lr.lm.Inc(commonmetrics.BackgroundSpanFetchFailureCount)
	return false, fmt.Errorf("error trying to fetch span with spanId = %d from layerDigest = %s: %w",
		lr.nextSpanFetchID, lr.layerDigest.String(), err)
}
