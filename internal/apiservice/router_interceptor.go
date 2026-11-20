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
	"strings"

	"github.com/agent-substrate/env/internal/ate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// guestCapsHeader is the response trailer key on which ate-env-guest
	// advertises supported wire capabilities.
	guestCapsHeader = "x-ate-env-guest-caps"
	// guestCapStartRequestID indicates that StartProcess deduplicates calls
	// carrying StartProcessRequest.request_id.
	guestCapStartRequestID = "start-request-id"
)

func (s *Server) recordGuestCaps(targetActor, capsHeader string) {
	if s.awakeEnvs == nil || targetActor == "" || capsHeader == "" {
		return
	}
	for _, part := range strings.Split(capsHeader, ",") {
		c := strings.TrimSpace(part)
		if c != "" {
			s.awakeEnvs.RecordCap(targetActor, c)
		}
	}
}

func (s *Server) hasGuestCap(targetActor, cap string) bool {
	if s.awakeEnvs == nil {
		return false
	}
	return s.awakeEnvs.HasCap(targetActor, cap)
}

func resolveTargetActor(ctx context.Context, targetActor string) string {
	if targetActor != "" {
		return targetActor
	}
	if ta, ok := ctx.Value(targetActorCtxKey{}).(string); ok {
		return ta
	}
	return ""
}

func (s *Server) routerUnaryInterceptor(targetActor string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		actor := resolveTargetActor(ctx, targetActor)
		if actor != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ate.TargetActorHeader, actor)
		}
		if !s.ReactiveWake {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		var trailerMD metadata.MD
		callOpts := append(opts, grpc.Trailer(&trailerMD))
		err := invoker(ctx, method, req, reply, cc, callOpts...)
		for _, capVal := range trailerMD.Get(guestCapsHeader) {
			s.recordGuestCaps(actor, capVal)
		}
		return err
	}
}

func (s *Server) routerStreamInterceptor(targetActor string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		actor := resolveTargetActor(ctx, targetActor)
		if actor != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ate.TargetActorHeader, actor)
		}
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil || !s.ReactiveWake {
			return cs, err
		}
		return &routerClientStream{
			ClientStream: cs,
			server:       s,
			targetActor:  actor,
		}, nil
	}
}

type routerClientStream struct {
	grpc.ClientStream
	server      *Server
	targetActor string
	observed    bool
}

func (s *routerClientStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err != nil && !s.observed {
		s.observed = true
		for _, capVal := range s.ClientStream.Trailer().Get(guestCapsHeader) {
			s.server.recordGuestCaps(s.targetActor, capVal)
		}
	}
	return err
}
