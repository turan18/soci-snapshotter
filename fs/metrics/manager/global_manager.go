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

type globalManager struct {
	imageManagers sync.Map
	globalMonitor monitor.Monitor
}

func (g *globalManager) AddManager(imgRef string, manager Manager) {
	g.imageManagers.Store(imgRef, manager)
}

func (g *globalManager) GetManager(imgRef string) (Manager, error) {
	v, ok := g.imageManagers.Load(imgRef)
	if !ok {
		return nil, fmt.Errorf("%w: %w", ErrNoManager, fmt.Errorf("no image manager with ref %s", imgRef))
	}
	return v.(Manager), nil
}

func (g *globalManager) RegisterRoot(m monitor.Monitor) {
	g.globalMonitor = m
}

func (g *globalManager) Root() (monitor.Monitor, error) {
	return g.globalMonitor, nil
}

// Unimplemented: globalManager only embeds a single root monitor.
func (g *globalManager) Register(_ string, m monitor.Monitor) {
}

// Unimplemented: globalManager only embeds a single root monitor.
func (g *globalManager) Get(_ string) (monitor.Monitor, error) {
	return nil, nil
}
