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

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// The span attribute keys of the environment or template an RPC targets.
// They are the keys of Substrate's internal/ateattr that ateapi stamps on
// its actor RPCs' server spans (cmd/ateapi/internal/controlapi/span_identity.go),
// so one query on ate.atespace and ate.actor.name finds an environment's
// ate-env-api spans and ateapi's spans of its actor. An environment's id is
// its actor's name.
const (
	attrAtespace         = attribute.Key("ate.atespace")
	attrActorName        = attribute.Key("ate.actor.name")
	attrActorUID         = attribute.Key("ate.actor.uid")
	attrActorVersion     = attribute.Key("ate.actor.version")
	attrTemplateName     = attribute.Key("ate.template.name")
	attrTemplateAtespace = attribute.Key("ate.template.atespace")
)

// setSpanNames sets each of names whose value is a resource name Substrate
// accepts (shortName, at most maxShortNameLen characters) on the RPC's
// server span (from ctx), and leaves the others as they were. ateapi
// validates a request before it stamps it; ate-env-api stamps before its
// call to ateapi, so a caller's id of megabytes, or empty, never reaches a
// span, and no actor has such a name to join. A no-op when ctx carries no
// recording span.
func setSpanNames(ctx context.Context, names ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(names))
	for _, kv := range names {
		if v := kv.Value.AsString(); len(v) <= maxShortNameLen && shortName.MatchString(v) {
			attrs = append(attrs, kv)
		}
	}
	span.SetAttributes(attrs...)
}

// setSpanAtespaceAttribute annotates the RPC's server span with the
// atespace a request reads, for a call that names no environment.
func setSpanAtespaceAttribute(ctx context.Context, atespace string) {
	setSpanNames(ctx, attrAtespace.String(atespace))
}

// setSpanEnvAttributes annotates the RPC's server span with the environment
// the request names, before the actor is read, so a failed call still
// carries it.
func setSpanEnvAttributes(ctx context.Context, atespace, id string) {
	setSpanNames(ctx, attrAtespace.String(atespace), attrActorName.String(id))
}

// setSpanActorAttributes is setSpanEnvAttributes with the actor's full
// identity, the set ateapi stamps (ateattr.ActorAttributes). A name that is
// empty leaves the attribute as it was, so an actor without a template
// reference keeps the template the request named.
func setSpanActorAttributes(ctx context.Context, a *ateapipb.Actor) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	setSpanNames(ctx,
		attrAtespace.String(a.GetMetadata().GetAtespace()),
		attrActorName.String(a.GetMetadata().GetName()),
		attrTemplateName.String(a.GetActorTemplate().GetName()),
		attrTemplateAtespace.String(a.GetActorTemplate().GetAtespace()),
	)
	attrs := []attribute.KeyValue{attrActorVersion.Int64(a.GetMetadata().GetVersion())}
	if uid := a.GetMetadata().GetUid(); uid != "" {
		attrs = append(attrs, attrActorUID.String(uid))
	}
	span.SetAttributes(attrs...)
}

// setSpanTemplateAttributes annotates the RPC's server span with the
// template it targets in atespace: the base template a request names, or
// the template derived for its rootfs image once the name is known. A
// later call replaces the template name.
func setSpanTemplateAttributes(ctx context.Context, atespace, name string) {
	setSpanNames(ctx, attrAtespace.String(atespace), attrTemplateName.String(name), attrTemplateAtespace.String(atespace))
}
