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
)

type Precision time.Duration

const (
	Milli Precision = iota
	Micro
)

// Monitor provides a general abstraction over Prometheus metric operations.
type Monitor interface {
	// Inc provides an abstraction over Prometheus counter/gauge increment operations.
	Inc(metric string)
	// Add provides an abstraction over Prometheus gauge add operations.
	Add(metric string, v int64)
	// Measure provides an abstraction over histogram observe operations.
	Measure(metric string, t time.Time, precision Precision)

	// Report can be used to implement custom metric operations. It can be used
	// in place of Inc, Add or Measure if the underlying metric represents
	// an error/failure, to be more expressive in denoting failures.
	Report(metric string)

	// Listen can be used in unison with `Report` to implement delayed/event
	// driven metrics operations.
	Listen(context.Context)
}
