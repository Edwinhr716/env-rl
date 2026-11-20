// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reporter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
)

// DirStore writes the bucket layout under a local directory, for tests and
// for a cluster without a bucket.
type DirStore struct{ Dir string }

// Write replaces Dir/name through a temporary file and a rename in the same
// directory. It refuses a name that is absolute or that leaves Dir.
func (s DirStore) Write(_ context.Context, name string, data []byte) (err error) {
	if local := filepath.FromSlash(name); !filepath.IsLocal(local) || filepath.Clean(local) == "." {
		return fmt.Errorf("reporter: object name %q is not a file under the directory", name)
	}
	target := filepath.Join(s.Dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp.Name())
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	// CreateTemp makes the file 0600; a snapshot is for other readers.
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), target)
}

// BucketStore writes to a Cloud Storage bucket. An upload becomes the object
// when it completes, so a reader never sees a part of it.
type BucketStore struct{ Bucket *storage.BucketHandle }

func (s BucketStore) Write(ctx context.Context, name string, data []byte) error {
	w := s.Bucket.Object(name).NewWriter(ctx)
	w.ContentType = "application/json"
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return fmt.Errorf("uploading %s: %w", name, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("uploading %s: %w", name, err)
	}
	return nil
}
