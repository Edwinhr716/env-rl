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

package apiservice_test

import (
	"context"
	"net"
	"testing"

	"github.com/agent-substrate/env/guest/process"
	"github.com/agent-substrate/env/internal/apiservice"
	"github.com/agent-substrate/env/internal/ate"
	"github.com/agent-substrate/env/internal/internaltest/fakecontrol"
	"github.com/agent-substrate/env/internal/internaltest/fakerouter"
	"github.com/agent-substrate/env/internal/unknownfields"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func newBenchTestEnv(b *testing.B) (*apiservice.Server, ateenvv1alpha.EnvironmentServiceClient, ateenvv1alpha.ProcessServiceClient, *fakerouter.Router) {
	b.Helper()

	control := fakecontrol.New()
	controlAddr, stopControl, err := control.Serve()
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(stopControl)

	router := fakerouter.New()
	router.Running = func(id string) bool {
		return control.Status(id) == ateapipb.ActorState_ACTOR_STATE_RUNNING
	}
	routerAddr, stopRouter := router.Serve()
	b.Cleanup(stopRouter)

	client, err := ate.New(ate.Options{
		ControlAddr: controlAddr,
		RouterAddr:  routerAddr,
		SkipVerify:  true,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { client.Close() })

	srv := apiservice.New(client, routerAddr, "")

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unknownfields.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(unknownfields.StreamServerInterceptor()),
	)
	ateenvv1alpha.RegisterEnvironmentServiceServer(grpcServer, srv)
	ateenvv1alpha.RegisterProcessServiceServer(grpcServer, srv)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcServer, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go grpcServer.Serve(lis)
	b.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { conn.Close() })

	return srv, ateenvv1alpha.NewEnvironmentServiceClient(conn), ateenvv1alpha.NewProcessServiceClient(conn), router
}

func BenchmarkGuestUnary(b *testing.B) {
	srv, envClient, procClient, router := newBenchTestEnv(b)
	srv.RouterPoolSize = 4
	srv.ReactiveWake = true
	b.Cleanup(srv.Close)

	ctx := context.Background()
	if _, err := envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "bench-env"}); err != nil {
		b.Fatalf("CreateEnvironment: %v", err)
	}

	tracker, err := process.NewTracker(process.TrackerConfig{
		LogDir:                 b.TempDir(),
		Workspace:              b.TempDir(),
		MaxConcurrentProcesses: 0,
		MaxRetainedProcesses:   64,
	})
	if err != nil {
		b.Fatalf("NewTracker: %v", err)
	}
	b.Cleanup(func() { tracker.Close() })

	guestGRPC := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(guestGRPC, process.NewService(tracker))
	b.Cleanup(guestGRPC.Stop)
	router.Register("bench-env", guestGRPC)

	envCtx := metadata.AppendToOutgoingContext(ctx,
		"x-env-id", "bench-env",
		"x-env-atespace", "ate-env",
	)

	seedProc, err := procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"true"},
	})
	if err != nil {
		b.Fatalf("seed StartProcess: %v", err)
	}
	getReq := &ateenvv1alpha.GetProcessRequest{ProcessId: seedProc.GetProcessId()}

	b.Run("GetProcess", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := procClient.GetProcess(envCtx, getReq); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("GetProcessParallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := procClient.GetProcess(envCtx, getReq); err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	startReq := &ateenvv1alpha.StartProcessRequest{
		Command: []string{"true"},
		Env: map[string]string{
			"K1": "v1",
			"K2": "v2",
			"K3": "v3",
			"K4": "v4",
		},
	}
	b.Run("StartProcess", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := procClient.StartProcess(envCtx, startReq); err != nil {
				b.Fatal(err)
			}
		}
	})
}
