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
	"errors"
	"sync"

	"github.com/awslabs/soci-snapshotter/fs/metrics/manager/monitor"
)

var (
	ErrNoManager        = errors.New("manager not found")
	ErrNoMonitor        = errors.New("monitor not found")
	ErrRootMonitorUnset = errors.New("root monitor is nil")
)

var gb *globalManager

// G returns the default Global observability manager.
var G = DefaultGlobalManager

// Manager manages monitors.
type Manager interface {
	// RegisterRoot registers the root `Monitor`.
	RegisterRoot(monitor.Monitor)
	// Root returns the root `Monitor`.
	Root() (monitor.Monitor, error)
	// Register registers a nested `Monitor` identified by key.
	Register(key string, m monitor.Monitor)
	// Get returns a nested `Monitor`` identified by key.
	Get(key string) (monitor.Monitor, error)
}

// MetaManager extends Manager. A MetaManager can manage other
// Manager's.
type MetaManager interface {
	// AddManager adds a `Manager` identified by key.
	AddManager(key string, m Manager)
	// GetManager returns a `Manager` identified by key.
	GetManager(key string) (Manager, error)
	Manager
}

func DefaultGlobalManager() MetaManager {
	return gb
}

func init() {
	// Create a new global observability manager.
	gb = &globalManager{
		imageManagers: sync.Map{},
	}
	// Register a global root monitor.
	gb.RegisterRoot(monitor.NewGlobalMonitor())
}
