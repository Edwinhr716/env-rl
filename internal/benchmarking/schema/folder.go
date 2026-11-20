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

package schema

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// TracedFolderSuffix marks the archive folder of a run that captured an SDK
// trace.
const TracedFolderSuffix = "_traced"

// folderNameRe is <YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced];
// the segments exclude "_" so the suffix is unambiguous.
var folderNameRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+_[a-zA-Z0-9.-]+(_traced)?$`)

// CheckFolderName checks the form of a benchmark bundle's folder name and its
// date, and returns whether the name marks a traced run. `bench upload`
// writes bundles under such names; the fleet site lists them by it.
func CheckFolderName(folder string) (traced bool, err error) {
	if !folderNameRe.MatchString(folder) {
		return false, fmt.Errorf(
			"invalid benchmark folder name %q: must match <YYYY-MM-DD>_<model-family>-<size>_<dataset>_<nickname>[_traced] (e.g. 2026-10-27_synthetic-w4_math-bb4dcec_suspend-sync, 2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced)",
			folder,
		)
	}
	if _, err := time.Parse("2006-01-02", folder[:10]); err != nil {
		return false, fmt.Errorf("invalid date prefix %q in folder name %q: %w", folder[:10], folder, err)
	}
	return strings.HasSuffix(folder, TracedFolderSuffix), nil
}
