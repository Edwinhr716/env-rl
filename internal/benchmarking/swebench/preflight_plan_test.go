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

package swebench

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/agent-substrate/env/internal/benchmarking/schema"
)

const (
	imgA = "registry.example/swebench/a@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	imgB = "registry.example/swebench/b@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	imgC = "registry.example/swebench/c@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	imgD = "registry.example/swebench/d@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

func twoDomains(budget int64) []schema.PlacementDomain {
	return []schema.PlacementDomain{
		{Name: "d0", Selector: map[string]string{"ate-env/domain": "d0"}, Nodes: 4, NodeCacheBytes: budget},
		{Name: "d1", Selector: map[string]string{"ate-env/domain": "d1"}, Nodes: 4, NodeCacheBytes: budget},
	}
}

func domainsOf(p *schema.ImagePlacement, image string) []string {
	if e := p.Entry(image); e != nil {
		return e.Domains
	}
	return nil
}

func TestPlanImagePlacementPacksLargestFirst(t *testing.T) {
	plan, err := PlanImagePlacement(PlanOptions{
		Instances: []Instance{
			{InstanceID: "c", Image: imgC, ImageBytesCompressed: 1},
			{InstanceID: "a", Image: imgA, ImageBytesCompressed: 3},
			{InstanceID: "b", Image: imgB, ImageBytesCompressed: 2},
		},
		Domains:      twoDomains(4),
		UnpackFactor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// a (3) takes d0, b (2) takes d1 (the emptier), c (1) takes d1 (2 free vs 1).
	want := map[string][]string{imgA: {"d0"}, imgB: {"d1"}, imgC: {"d1"}}
	for img, doms := range want {
		if got := domainsOf(plan, img); !reflect.DeepEqual(got, doms) {
			t.Errorf("%s placed in %v, want %v", img, got, doms)
		}
	}
	if d := plan.Domain("d0"); d.AssignedBytes != 3 || d.ImageCount != 1 {
		t.Errorf("d0 = %+v, want 3 bytes, 1 image", *d)
	}
	if d := plan.Domain("d1"); d.AssignedBytes != 3 || d.ImageCount != 2 {
		t.Errorf("d1 = %+v, want 3 bytes, 2 images", *d)
	}
	if len(plan.Unplaced) != 0 {
		t.Errorf("unplaced = %v, want none", plan.Unplaced)
	}
	// Images come out sorted by reference, whatever the packing order.
	for i := 1; i < len(plan.Images); i++ {
		if plan.Images[i-1].Image > plan.Images[i].Image {
			t.Fatalf("images not sorted: %v", plan.Images)
		}
	}
	again, err := PlanImagePlacement(PlanOptions{
		Instances:    []Instance{{Image: imgB, ImageBytesCompressed: 2}, {Image: imgC, ImageBytesCompressed: 1}, {Image: imgA, ImageBytesCompressed: 3}},
		Domains:      twoDomains(4),
		UnpackFactor: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, plan) {
		t.Error("the plan depends on the manifest's order")
	}
}

func TestPlanImagePlacementReplicationAndUnpackFactor(t *testing.T) {
	plan, err := PlanImagePlacement(PlanOptions{
		Instances:   []Instance{{Image: imgA, ImageBytesCompressed: 10}, {Image: imgB, ImageBytesCompressed: 10}},
		Domains:     twoDomains(100),
		Replication: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, img := range []string{imgA, imgB} {
		if got := domainsOf(plan, img); !reflect.DeepEqual(got, []string{"d0", "d1"}) {
			t.Errorf("%s placed in %v, want both domains", img, got)
		}
	}
	if e := plan.Entry(imgA); e.BytesUnpacked != 25 || plan.UnpackFactor != DefaultUnpackFactor {
		t.Errorf("unpacked = %d with factor %v, want 25 with %v", e.BytesUnpacked, plan.UnpackFactor, DefaultUnpackFactor)
	}
	// Replication beyond the domain count is capped, not an error.
	plan, err = PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgA, ImageBytesCompressed: 1}}, Domains: twoDomains(0), Replication: 5})
	if err != nil {
		t.Fatal(err)
	}
	if got := domainsOf(plan, imgA); len(got) != 2 {
		t.Errorf("replication 5 over 2 domains placed in %v", got)
	}
}

func TestPlanImagePlacementUnplacedAndAssumedSizes(t *testing.T) {
	plan, err := PlanImagePlacement(PlanOptions{
		Instances: []Instance{
			{Image: imgA, ImageBytesCompressed: 50}, // fits nowhere at factor 1 with budget 40
			{Image: imgB},                           // no size: the default stands in
			{Image: imgC, ImageBytesCompressed: 30},
		},
		Domains:                twoDomains(40),
		UnpackFactor:           1,
		DefaultBytesCompressed: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Unplaced) != 1 || plan.Unplaced[0].Image != imgA || !strings.Contains(plan.Unplaced[0].Reason, "needs 50") {
		t.Errorf("unplaced = %+v, want a (needs 50)", plan.Unplaced)
	}
	if e := plan.Entry(imgB); e == nil || !e.SizeAssumed || e.BytesCompressed != 5 || len(e.Domains) != 1 {
		t.Errorf("b = %+v, want placed with the assumed size 5", e)
	}
	if e := plan.Entry(imgC); e == nil || e.SizeAssumed {
		t.Errorf("c = %+v, want placed with its own size", e)
	}

	// Without a default, an image without a size is unplaced and says so.
	plan, err = PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgB}}, Domains: twoDomains(0), UnpackFactor: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Unplaced) != 1 || !strings.Contains(plan.Unplaced[0].Reason, "no image_bytes_compressed") {
		t.Errorf("unplaced = %+v, want b for lack of a size", plan.Unplaced)
	}

	// Replication 2 on a budget that holds one image per domain: each image
	// gets one domain (coverage before replicas), none is unplaced.
	plan, err = PlanImagePlacement(PlanOptions{
		Instances:    []Instance{{Image: imgA, ImageBytesCompressed: 30}, {Image: imgB, ImageBytesCompressed: 30}},
		Domains:      twoDomains(40),
		UnpackFactor: 1,
		Replication:  2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Unplaced) != 0 || len(domainsOf(plan, imgA)) != 1 || len(domainsOf(plan, imgB)) != 1 {
		t.Errorf("plan = %+v, want a and b in one domain each", plan.Images)
	}
	if under := UnderReplicated(plan, 0, 0); len(under) != 2 {
		t.Errorf("under-replicated = %v, want both", under)
	}
}

func TestPlanImagePlacementDemandFromSchedule(t *testing.T) {
	sched := &schema.Schedule{
		SchemaVersion: schema.SchemaVersion,
		ScenarioName:  "s",
		Jobs: []schema.ScheduledJob{{
			Job: "j",
			Steps: []schema.ScheduledStep{
				{Step: 1, Tasks: []schema.ScheduledTask{{InstanceID: "a", Image: imgA}, {InstanceID: "a", Image: imgA, Rollout: 1}, {InstanceID: "b", Image: imgB}}},
				{Step: 2, Tasks: []schema.ScheduledTask{{InstanceID: "a", Image: imgA}, {InstanceID: "a", Image: imgA, Rollout: 1}, {InstanceID: "a", Image: imgA, Rollout: 2}}},
			},
		}},
	}
	plan, err := PlanImagePlacement(PlanOptions{
		// d is in the manifest but not in the schedule: not placed.
		Instances:      []Instance{{Image: imgA, ImageBytesCompressed: 1}, {Image: imgB, ImageBytesCompressed: 1}, {Image: imgD, ImageBytesCompressed: 1}},
		Schedule:       sched,
		Domains:        twoDomains(0),
		UnpackFactor:   1,
		HotDemand:      3,
		HotReplication: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if e := plan.Entry(imgA); e == nil || e.Demand != 3 || len(e.Domains) != 2 {
		t.Errorf("a = %+v, want demand 3 and two domains (hot)", e)
	}
	if e := plan.Entry(imgB); e == nil || e.Demand != 1 || len(e.Domains) != 1 {
		t.Errorf("b = %+v, want demand 1 and one domain", e)
	}
	if plan.Entry(imgD) != nil {
		t.Error("d is not in the schedule but was placed")
	}
}

func TestReadPlacementDomainsAndWriteReadPlan(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("domains.json", `{"domains":[{"name":"d0","selector":{"ate-env/domain":"d0"},"nodes":4,"node_cache_bytes":100}]}`)
	domains, _, err := ReadPlacementDomains(good)
	if err != nil || len(domains) != 1 || domains[0].Nodes != 4 {
		t.Fatalf("ReadPlacementDomains = %v, %v", domains, err)
	}
	// The output of `ate-env placement-domains --json`: workers and
	// worker_namespace, no node_cache_bytes, and node_slots one worker's
	// configured capacity. The reader's per-node value is min(capacity,
	// DefaultActorsPerWorker) x workers per node: 8 workers on 4 nodes with
	// capacity 10 give 3 x 2 = 6, with capacity 1 give 1 x 2 = 2 (not the
	// capacity alone, which the pre-flight would refuse two probes on), with
	// no capacity 3 x 2 = 6; 16 workers on 4 nodes with capacity 10 give 12;
	// an uneven pool rounds its workers per node down (5 on 2 nodes: 2), at
	// least one (1 worker on 4 nodes: 3).
	api := write("api.json", `{"domains":[`+
		`{"name":"pool-d0","worker_namespace":"ate-system","selector":{"ate-env/domain":"d0"},"nodes":4,"workers":8,"node_slots":10},`+
		`{"name":"pool-d1","worker_namespace":"ate-system","selector":{"ate-env/domain":"d1"},"nodes":4,"workers":8,"node_slots":1},`+
		`{"name":"pool-d2","worker_namespace":"ate-system","selector":{"ate-env/domain":"d2"},"nodes":4,"workers":8},`+
		`{"name":"pool-d3","worker_namespace":"ate-system","selector":{"ate-env/domain":"d3"},"nodes":4,"workers":16,"node_slots":10},`+
		`{"name":"pool-d4","worker_namespace":"ate-system","selector":{"ate-env/domain":"d4"},"nodes":2,"workers":5,"node_slots":12},`+
		`{"name":"pool-d5","worker_namespace":"ate-system","selector":{"ate-env/domain":"d5"},"nodes":4,"workers":1,"node_slots":12}]}`)
	fromAPI, apiSkipped, err := ReadPlacementDomains(api)
	if err != nil {
		t.Fatalf("ReadPlacementDomains(api output): %v", err)
	}
	for i, want := range []int{6, 2, 6, 12, 6, 3} {
		if fromAPI[i].NodeSlots != want {
			t.Errorf("%s node_slots = %d, want %d", fromAPI[i].Name, fromAPI[i].NodeSlots, want)
		}
	}
	if fromAPI[0].Workers != 8 || fromAPI[0].WorkerNamespace != "ate-system" || fromAPI[0].NodeCacheBytes != 0 {
		t.Errorf("pool-d0 = %+v, want workers and worker_namespace kept and no budget", fromAPI[0])
	}
	// A plan over the API's domains validates against the JSON schema with
	// the two fields in it.
	apiPlan, err := PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgA, ImageBytesCompressed: 7}}, Domains: fromAPI})
	if err != nil {
		t.Fatal(err)
	}
	rawPlan, err := json.Marshal(apiPlan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rawPlan), `"workers":8`) || !strings.Contains(string(rawPlan), `"worker_namespace":"ate-system"`) {
		t.Errorf("plan JSON lacks the API's fields:\n%s", rawPlan)
	}
	if err := schema.ValidateAgainstJSONSchema("image_placement.schema.json", rawPlan); err != nil {
		t.Errorf("plan over the API's domains against the schema: %v", err)
	}
	if len(apiSkipped) != 0 {
		t.Errorf("skipped = %v, want none", apiSkipped)
	}
	// A pool of the API's output that no selector addresses is skipped with
	// its reason and the rest of the file is read: workers that report no
	// node, a shared label with an empty value, workers that share no label
	// (a file made without --template). The order of the file is kept.
	mixed, skipped, err := ReadPlacementDomains(write("mixed.json", `{"domains":[`+
		`{"name":"pool-nonode","worker_namespace":"ate-system","selector":{"ate-env/domain":"x"},"nodes":0,"workers":2,"node_slots":10},`+
		`{"name":"pool-ok","worker_namespace":"ate-system","selector":{"ate-env/domain":"d0"},"nodes":4,"workers":8,"node_slots":10},`+
		`{"name":"pool-emptyval","worker_namespace":"ate-system","selector":{"canary":"","ate-env/domain":"y"},"nodes":1,"workers":1,"node_slots":10},`+
		`{"name":"pool-nolabel","worker_namespace":"ns-b","selector":{},"nodes":2,"workers":2},`+
		`{"name":"pool-nolabel-nons","selector":{},"nodes":2,"workers":2}]}`))
	if err != nil {
		t.Fatalf("ReadPlacementDomains(mixed): %v", err)
	}
	if len(mixed) != 1 || mixed[0].Name != "pool-ok" || mixed[0].NodeSlots != 6 {
		t.Errorf("domains = %+v, want pool-ok alone with node_slots 6", mixed)
	}
	var got []string
	for _, s := range skipped {
		got = append(got, s.String())
	}
	wantSkipped := []string{
		"ate-system/pool-nonode: its workers report no node",
		`ate-system/pool-emptyval: its label "canary" has an empty value, which a selector cannot name`,
		"ns-b/pool-nolabel: its workers share no label, so no selector addresses it",
		"pool-nolabel-nons: its workers share no label, so no selector addresses it",
	}
	if !reflect.DeepEqual(got, wantSkipped) {
		t.Errorf("skipped =\n%q\nwant\n%q", got, wantSkipped)
	}
	// A file whose pools are all skipped is an error that names each.
	_, _, err = ReadPlacementDomains(write("allskipped.json", `{"domains":[`+
		`{"name":"pool-a","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":2},`+
		`{"name":"pool-b","worker_namespace":"ate-system","selector":{},"nodes":1,"workers":1}]}`))
	if err == nil || !strings.Contains(err.Error(), "no pool is addressable") || !strings.Contains(err.Error(), "ate-system/pool-a: its workers report no node") || !strings.Contains(err.Error(), "ate-system/pool-b: its workers share no label") {
		t.Errorf("all pools skipped: err = %v, want each pool and its reason", err)
	}
	const okPool = `{"name":"pool-ok","worker_namespace":"ate-system","selector":{"ate-env/domain":"d0"},"nodes":4,"workers":8,"node_slots":10}`
	// What still refuses an API file: one name in two namespaces, also when
	// one of the two would be skipped; a worker count that overflows
	// node_slots; a negative node count; planner fields set. And a
	// hand-written domain (no workers, no worker_namespace) with a skipped
	// pool's fault is a mistake in the file, not a pool's state.
	for name, body := range map[string]string{
		"dupname.json":     `{"domains":[{"name":"pool-x","worker_namespace":"ns-a","selector":{"a":"b"},"nodes":1,"workers":1},{"name":"pool-x","worker_namespace":"ns-b","selector":{"a":"c"},"nodes":1,"workers":1}]}`,
		"dupskipped.json":  `{"domains":[{"name":"pool-x","worker_namespace":"ns-a","selector":{"a":"b"},"nodes":0,"workers":1},{"name":"pool-x","worker_namespace":"ns-b","selector":{"a":"c"},"nodes":1,"workers":1}]}`,
		"overflow.json":    `{"domains":[{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":1,"workers":9223372036854775807}]}`,
		"negnodes.json":    `{"domains":[{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":-1,"workers":1}]}`,
		"apifilled.json":   `{"domains":[{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":1,"workers":1,"image_count":2}]}`,
		"noname.json":      `{"domains":[{"worker_namespace":"ate-system","selector":{},"nodes":1,"workers":1}]}`,
		"hand-nonode.json": `{"domains":[{"name":"d0","selector":{"a":"b"},"nodes":0}]}`,
		"hand-empty.json":  `{"domains":[{"name":"d0","selector":{"a":""},"nodes":1}]}`,
	} {
		if _, _, err := ReadPlacementDomains(write(name, body)); err == nil {
			t.Errorf("%s accepted, want an error", name)
		}
	}
	// A fault of the file is refused in a pool that would be skipped too: the
	// skip is for a state of the pool's workers, not for a malformed entry.
	// Each of these has no node (or no shared label, or an empty label
	// value) and one fault: a negative count, a planner field set, an empty
	// selector key, a worker count that overflows node_slots.
	for name, body := range map[string]string{
		"skip-negworkers.json":   `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":-3}]}`,
		"skip-negslots.json":     `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":2,"node_slots":-1}]}`,
		"skip-negcache.json":     `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{},"nodes":1,"workers":2,"node_cache_bytes":-1}]}`,
		"skip-assigned.json":     `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":2,"assigned_bytes":500}]}`,
		"skip-imagecount.json":   `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":""},"nodes":1,"workers":2,"image_count":2}]}`,
		"skip-negassigned.json":  `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":2,"assigned_bytes":-5}]}`,
		"skip-emptykey.json":     `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"":"b"},"nodes":0,"workers":2}]}`,
		"skip-emptykeyval.json":  `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"":""},"nodes":1,"workers":2}]}`,
		"skip-overflow.json":     `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{"a":"b"},"nodes":0,"workers":9223372036854775807}]}`,
		"skip-overflow-nol.json": `{"domains":[` + okPool + `,{"name":"pool-x","worker_namespace":"ate-system","selector":{},"nodes":1,"workers":9223372036854775807,"node_slots":2}]}`,
	} {
		if _, _, err := ReadPlacementDomains(write(name, body)); err == nil {
			t.Errorf("%s accepted, want an error", name)
		} else if !strings.Contains(err.Error(), "domains[1]") {
			t.Errorf("%s: err = %v, want it to name domains[1]", name, err)
		}
	}
	for name, body := range map[string]string{
		"filled.json": `{"domains":[{"name":"d0","selector":{"a":"b"},"nodes":1,"node_cache_bytes":0,"assigned_bytes":5}]}`,
		"negw.json":   `{"domains":[{"name":"d0","selector":{"a":"b"},"nodes":1,"workers":-1}]}`,
		"dup.json":    `{"domains":[{"name":"d0","selector":{"a":"b"},"nodes":1},{"name":"d0","selector":{"a":"c"},"nodes":1}]}`,
		"nosel.json":  `{"domains":[{"name":"d0","nodes":1}]}`,
		"empty.json":  `{"domains":[]}`,
		"extra.json":  `{"domains":[{"name":"d0","selector":{"a":"b"},"nodes":1}],"other":1}`,
	} {
		if _, _, err := ReadPlacementDomains(write(name, body)); err == nil {
			t.Errorf("%s accepted, want an error", name)
		}
	}

	plan, err := PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgA, ImageBytesCompressed: 7}}, Domains: domains})
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "sub", "image-placement.json")
	if err := WriteImagePlacement(out, plan); err != nil {
		t.Fatal(err)
	}
	back, err := ReadImagePlacement(out)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, plan) {
		t.Errorf("round trip changed the plan:\n%+v\n%+v", back, plan)
	}
}

func TestParseByteSize(t *testing.T) {
	cases := map[string]int64{
		"4096": 4096, "4096B": 4096, "1K": 1024, "1KiB": 1024, "1KB": 1000, "3G": 3 << 30, "3GiB": 3 << 30,
		"3GB": 3_000_000_000, "1.5TB": 1_500_000_000_000, "0.5MiB": 512 * 1024, " 2 g ": 2 << 30,
	}
	for in, want := range cases {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "GiB", "1X", "-1G", "1.2.3G"} {
		if _, err := ParseByteSize(bad); err == nil {
			t.Errorf("ParseByteSize(%q) accepted", bad)
		}
	}
}

func TestPlanImagePlacementRejectsNonFiniteFactorsAndOverflow(t *testing.T) {
	for _, factor := range []float64{math.NaN(), math.Inf(1), -1} {
		if _, err := PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgA, ImageBytesCompressed: 1}}, Domains: twoDomains(0), UnpackFactor: factor}); err == nil {
			t.Errorf("factor %v accepted", factor)
		}
	}
	if _, err := PlanImagePlacement(PlanOptions{Instances: []Instance{{Image: imgA, ImageBytesCompressed: math.MaxInt64}}, Domains: twoDomains(0)}); err == nil || !strings.Contains(err.Error(), "overflows") {
		t.Errorf("an overflowing unpacked size was accepted: %v", err)
	}
	for _, bad := range []string{"1e20GiB", "nan", "inf", "9223372036854775807KiB"} {
		if _, err := ParseByteSize(bad); err == nil {
			t.Errorf("ParseByteSize(%q) accepted", bad)
		}
	}
}
