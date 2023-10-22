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
	"testing"

	"github.com/awslabs/soci-snapshotter/fs/metrics/manager/monitor"
	"github.com/opencontainers/go-digest"
)

func TestMetaManagerAPI(t *testing.T) {
	// MetaManager API
	// Test AddManager, GetManager

	imgRef := "example.com/image"
	dummyGlobalManager := &globalManager{}
	dummyImageManager := NewImageManager()

	dummyGlobalManager.AddManager(imgRef, dummyImageManager)
	_, err := dummyGlobalManager.GetManager(imgRef)
	if err != nil {
		t.Fatal(err)
	}

}
func TestGlobalManager(t *testing.T) {
	// Test RegisterRoot and Root for global manager.

	dummyGlobalManager := &globalManager{}

	dummyGlobalManager.RegisterRoot(monitor.NewGlobalMonitor())
	_, err := dummyGlobalManager.Root()
	if err != nil {
		t.Fatal(err)
	}
}

func TestImageManager(t *testing.T) {
	// Test RegisterRoot, Root, Register, and Get for image manager.
	imgDigest := digest.FromString("abc")
	layerDigest := digest.FromString("def")
	dummyImageManager := NewImageManager()

	// Test RegisterRoot and Root
	dummyImageManager.RegisterRoot(monitor.NewImageMonitor(imgDigest))
	_, err := dummyImageManager.Root()
	if err != nil {
		t.Fatal(err)
	}

	// Test Register and Get
	dummyImageManager.Register(string(layerDigest), monitor.NewLayerMonitor(layerDigest))
	_, err = dummyImageManager.Get(string(layerDigest))
	if err != nil {
		t.Fatal(err)
	}
}
