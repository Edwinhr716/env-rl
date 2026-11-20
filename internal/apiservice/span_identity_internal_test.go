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

package apiservice

import (
	"context"
	"testing"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// FS-16: an actor that comes back without a template reference or a uid
// leaves the template the request named and the uid stamped before on the
// span, and its version is an int64, as ateapi stamps it.
func TestSetSpanActorAttributesKeepsKnownValues(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	ctx, span := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test").Start(context.Background(), "rpc")
	setSpanTemplateAttributes(ctx, "ate-env", "base")
	setSpanActorAttributes(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "e1", Uid: "u1", Version: 3}})
	// An actor read without a uid leaves the uid stamped before.
	setSpanActorAttributes(ctx, &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "ate-env", Name: "e1", Version: 3}})
	span.End()
	got := map[string]string{}
	for _, kv := range rec.Ended()[0].Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
		if kv.Key == "ate.actor.version" && kv.Value.Type() != attribute.INT64 {
			t.Errorf("ate.actor.version is a %v, want an int64", kv.Value.Type())
		}
	}
	want := map[string]string{
		"ate.atespace": "ate-env", "ate.actor.name": "e1", "ate.actor.uid": "u1", "ate.actor.version": "3",
		"ate.template.name": "base", "ate.template.atespace": "ate-env",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("attributes %v, want %v", got, want)
	}
}
