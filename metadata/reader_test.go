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

/*
   Copyright The containerd Authors.

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

package metadata

import (
	"compress/gzip"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/awslabs/soci-snapshotter/util/testutil"
	"github.com/awslabs/soci-snapshotter/ztoc"
	"go.etcd.io/bbolt"
)

var allowedPrefix = [4]string{"", "./", "/", "../"}

var srcCompressions = map[string]int{
	"gzip-nocompression":      gzip.NoCompression,
	"gzip-bestspeed":          gzip.BestSpeed,
	"gzip-bestcompression":    gzip.BestCompression,
	"gzip-defaultcompression": gzip.DefaultCompression,
	"gzip-huffmanonly":        gzip.HuffmanOnly,
}

func TestMetadataReader(t *testing.T) {
	sampleTime := time.Now().Truncate(time.Second)
	tests := []struct {
		name string
		in   []testutil.TarEntry
		want []check
	}{
		{
			name: "files",
			in: []testutil.TarEntry{
				testutil.File("foo", "foofoo", testutil.WithFileMode(0644|os.ModeSetuid)),
				testutil.Dir("bar/"),
				testutil.File("bar/baz.txt", "bazbazbaz", testutil.WithFileOwner(1000, 1000)),
				testutil.File("xxx.txt", "xxxxx", testutil.WithFileModTime(sampleTime)),
				testutil.File("y.txt", "", testutil.WithFileXattrs(map[string]string{"testkey": "testval"})),
			},
			want: []check{
				numOfNodes(6), // root dir + 1 dir + 4 files
				hasFile("foo", 6),
				hasMode("foo", 0644|os.ModeSetuid),
				hasFile("bar/baz.txt", 9),
				hasOwner("bar/baz.txt", 1000, 1000),
				hasFile("xxx.txt", 5),
				hasModTime("xxx.txt", sampleTime),
				hasFile("y.txt", 0),
				// For details on the keys of Xattrs, see https://pkg.go.dev/archive/tar#Header
				hasXattrs("y.txt", map[string]string{"testkey": "testval"}),
			},
		},
		{
			name: "dirs",
			in: []testutil.TarEntry{
				testutil.Dir("foo/", testutil.WithDirMode(os.ModeDir|0600|os.ModeSticky)),
				testutil.Dir("foo/bar/", testutil.WithDirOwner(1000, 1000)),
				testutil.File("foo/bar/baz.txt", "testtest"),
				testutil.File("foo/bar/xxxx", "x"),
				testutil.File("foo/bar/yyy", "yyy"),
				testutil.Dir("foo/a/", testutil.WithDirModTime(sampleTime)),
				testutil.Dir("foo/a/1/", testutil.WithDirXattrs(map[string]string{"testkey": "testval"})),
				testutil.File("foo/a/1/2", "1111111111"),
			},
			want: []check{
				numOfNodes(9), // root dir + 4 dirs + 4 files
				hasDirChildren("foo", "bar", "a"),
				hasDirChildren("foo/bar", "baz.txt", "xxxx", "yyy"),
				hasDirChildren("foo/a", "1"),
				hasDirChildren("foo/a/1", "2"),
				hasMode("foo", os.ModeDir|0600|os.ModeSticky),
				hasOwner("foo/bar", 1000, 1000),
				hasModTime("foo/a", sampleTime),
				hasXattrs("foo/a/1", map[string]string{"testkey": "testval"}),
				hasFile("foo/bar/baz.txt", 8),
				hasFile("foo/bar/xxxx", 1),
				hasFile("foo/bar/yyy", 3),
				hasFile("foo/a/1/2", 10),
			},
		},
		{
			name: "hardlinks",
			in: []testutil.TarEntry{
				testutil.File("foo", "foofoo", testutil.WithFileOwner(1000, 1000)),
				testutil.Dir("bar/"),
				testutil.Link("bar/foolink", "foo"),
				testutil.Link("bar/foolink2", "bar/foolink"),
				testutil.Dir("bar/1/"),
				testutil.File("bar/1/baz.txt", "testtest"),
				testutil.Link("barlink", "bar/1/baz.txt"),
				testutil.Symlink("foosym", "bar/foolink2"),
			},
			want: []check{
				numOfNodes(6), // root dir + 2 dirs + 1 flie(linked) + 1 file(linked) + 1 symlink
				hasFile("foo", 6),
				hasOwner("foo", 1000, 1000),
				hasFile("bar/foolink", 6),
				hasOwner("bar/foolink", 1000, 1000),
				hasFile("bar/foolink2", 6),
				hasOwner("bar/foolink2", 1000, 1000),
				hasFile("bar/1/baz.txt", 8),
				hasFile("barlink", 8),
				hasDirChildren("bar", "foolink", "foolink2", "1"),
				hasDirChildren("bar/1", "baz.txt"),
				sameNodes("foo", "bar/foolink", "bar/foolink2"),
				sameNodes("bar/1/baz.txt", "barlink"),
				linkName("foosym", "bar/foolink2"),
				hasNumLink("foo", 3),     // parent dir + 2 links
				hasNumLink("barlink", 2), // parent dir + 1 link
				hasNumLink("bar", 3),     // parent + "." + child's ".."
			},
		},
		{
			name: "various files",
			in: []testutil.TarEntry{
				testutil.Dir("bar/"),
				testutil.File("bar/../bar///////////////////foo", ""),
				testutil.Chardev("bar/cdev", 10, 11),
				testutil.Blockdev("bar/bdev", 100, 101),
				testutil.Fifo("bar/fifo"),
			},
			want: []check{
				numOfNodes(6), // root dir + 1 file + 1 dir + 1 cdev + 1 bdev + 1 fifo
				hasFile("bar/foo", 0),
				hasChardev("bar/cdev", 10, 11),
				hasBlockdev("bar/bdev", 100, 101),
				hasFifo("bar/fifo"),
			},
		},
	}
	for _, tt := range tests {
		for _, prefix := range allowedPrefix {
			prefix := prefix
			for srcCompresionName, srcCompression := range srcCompressions {
				t.Run(tt.name+"-"+srcCompresionName, func(t *testing.T) {
					opts := []testutil.BuildTarOption{
						testutil.WithPrefix(prefix),
					}

					ztoc, sr, err := ztoc.BuildZtocReader(t, tt.in, srcCompression, 64, opts...)
					if err != nil {
						t.Fatalf("failed to build ztoc: %v", err)
					}
					telemetry, checkCalled := newCalledTelemetry()

					// create a metadata reader
					r, err := newTestableReader(sr, ztoc.TOC, WithTelemetry(telemetry))
					if err != nil {
						t.Fatalf("failed to create new reader: %v", err)
					}
					defer r.Close()
					t.Logf("vvvvv Node tree vvvvv")
					t.Logf("[%d] ROOT", r.RootID())
					dumpNodes(t, r, r.RootID(), 1)
					t.Logf("^^^^^^^^^^^^^^^^^^^^^")
					for _, want := range tt.want {
						want(t, r)
					}
					if err := checkCalled(); err != nil {
						t.Errorf("telemetry failure: %v", err)
					}
				})
			}
		}
	}
}
func BenchmarkMetadataReader(b *testing.B) {

	newTempDB := func() (*bbolt.DB, func(), error) {
		f, err := os.CreateTemp("", "readertestdb")
		if err != nil {
			return nil, nil, err
		}
		db, err := bbolt.Open(f.Name(), 0600, nil)
		if err != nil {
			return nil, func() { os.Remove(f.Name()) }, err
		}
		return db, func() {
			db.Close()
			os.Remove(f.Name())
		}, err
	}

	generateTOC := func(numEntries int) (ztoc.TOC, error) {
		tarEntries := testutil.GenerateFileTarEntries(numEntries)
		ztoc, _, err := ztoc.BuildZtocReader(nil, tarEntries, gzip.DefaultCompression, 4<<20)
		temp := *ztoc
		return temp.TOC, err
	}

	tempDB, clean, err := newTempDB()
	defer clean()
	if err != nil {
		b.Fatalf("failed to initialize temp db: %v", err)
	}

	testCases := []struct {
		name    string
		entries int
	}{
		// {
		// 	name:    "Create metadata.Reader with few TOC entries",
		// 	entries: 1000,
		// },
		// {
		// 	name:    "Create metadata.Reader with a good amount TOC entries",
		// 	entries: 10_000,
		// },
		// {
		// 	name:    "Create metadata.Reader with many TOC entries",
		// 	entries: 50_000,
		// },
		{
			name:    "Create metadata.Reader with an enormous amount of TOC entries",
			entries: 100_000,
		},
	}
	for _, tc := range testCases {
		toc, err := generateTOC(tc.entries)
		if err != nil {
			b.Fatalf("failed to generate TOC: %v", err)
		}
		b.Run(tc.name, func(b *testing.B) {
			_, err := NewReader(tempDB, nil, toc)
			fmt.Fprintf(os.Stderr, "DONE\n")
			if err != nil {
				b.Fatalf("failed to create new reader: %v", err)
			}
		})
	}
}

// func BenchmarkConcurrentMetadataReader(b *testing.B) {
// 	testCases := []struct {
// 		name string
// 	}{}
// 	fmt.Println(testCases)
// }
