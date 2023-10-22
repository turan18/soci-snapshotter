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

package manager

import (
	"fmt"
	"sync"

	"github.com/awslabs/soci-snapshotter/fs/metrics/manager/monitor"
)

type imageManager struct {
	layerMonitors sync.Map
	imageMonitor  monitor.Monitor
}

func (i *imageManager) RegisterRoot(m monitor.Monitor) {
	i.imageMonitor = m
}

func (i *imageManager) Root() (monitor.Monitor, error) {
	if i.imageMonitor == nil {
		return nil, fmt.Errorf("%w: %w", ErrNoMonitor, ErrRootMonitorUnset)
	}
	return i.imageMonitor, nil
}

func (i *imageManager) Register(layerDigest string, m monitor.Monitor) {
	i.layerMonitors.Store(layerDigest, m)
}

func (i *imageManager) Get(digest string) (monitor.Monitor, error) {
	v, ok := i.layerMonitors.Load(digest)
	if !ok {
		return nil, fmt.Errorf("%w: %w", ErrNoMonitor, fmt.Errorf("no layer monitor with digest %s", digest))
	}
	return v.(monitor.Monitor), nil

}

// NewImageManager returns a a new imageManager. An imageManager contains
// a single root image monitor as well nested layer monitors for each
// layer in an image.
func NewImageManager() Manager {
	return &imageManager{
		layerMonitors: sync.Map{},
	}
}
