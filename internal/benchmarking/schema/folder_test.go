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

import "testing"

func TestCheckFolderName(t *testing.T) {
	for folder, traced := range map[string]bool{
		"2026-10-27_synthetic-w4_math-bb4dcec_suspend-sync":    false,
		"2026-10-28_qwen3.5-35b_r2e-gym-easy_prewarmed_traced": true,
	} {
		got, err := CheckFolderName(folder)
		if err != nil || got != traced {
			t.Errorf("%s: traced %v, %v; want %v", folder, got, err, traced)
		}
	}
	for _, folder := range []string{"", "not-a-bundle", "2026-13-40_a_b_c", "2026-10-27_a_b", "2026-10-27_a_b_c_d_e", "2026-10-27_a/b_c_d", "../2026-10-27_a_b_c"} {
		if _, err := CheckFolderName(folder); err == nil {
			t.Errorf("%q accepted", folder)
		}
	}
}
