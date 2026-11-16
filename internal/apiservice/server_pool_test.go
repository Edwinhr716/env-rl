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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/guest"
	"github.com/agent-substrate/env/guest/process"
	"github.com/agent-substrate/env/internal/ate"
	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// installWakeOnRouterRequest makes router behave like atenet-router when
// reaching a suspended actor: a request to a SUSPENDED actor triggers a
// background control-plane ResumeActor while returning 503 Unavailable on that
// first attempt, and subsequent attempts see ACTOR_STATE_RUNNING.
func installWakeOnRouterRequest(te *testEnv) {
	te.router.Running = func(id string) bool {
		switch te.control.Status(id) {
		case ateapipb.ActorState_ACTOR_STATE_RUNNING:
			return true
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			_, _ = te.control.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
				Actor: &ateapipb.ObjectRef{Atespace: "ate-env", Name: id},
			})
		}
		return false
	}
}

// TestDefaultServerLeavesWireUnchanged checks WF-9 (unchanged wire traffic,
// empty awakeEnvs when ReactiveWake is off, and per-call Close rejection).
func TestDefaultServerLeavesWireUnchanged(t *testing.T) {
	te := newFullTestEnv(t)
	// Default server: RouterPoolSize == 0, ReactiveWake == false.
	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "default-env"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	var (
		mu       sync.Mutex
		gotReqID []string
		gotMD    []metadata.MD
	)
	stub := &inspectingGuestServer{
		onStart: func(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			mu.Lock()
			gotReqID = append(gotReqID, req.GetRequestId())
			gotMD = append(gotMD, md.Copy())
			mu.Unlock()
			return &ateenvv1alpha.Process{ProcessId: "p-1"}, nil
		},
	}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, stub)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register("default-env", grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "default-env", "x-env-atespace", "ate-env")

	// 1. Empty request_id stays empty on the wire (does not populate the guest idempotency table).
	if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"true"},
	}); err != nil {
		t.Fatalf("StartProcess without request_id: %v", err)
	}

	// 2. Explicit caller request_id passes through untouched even when the guest never sent x-ate-env-guest-caps.
	if _, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"true"},
		RequestId: "caller-explicit-id",
	}); err != nil {
		t.Fatalf("StartProcess with caller request_id: %v", err)
	}

	mu.Lock()
	if len(gotReqID) != 2 || gotReqID[0] != "" || gotReqID[1] != "caller-explicit-id" {
		mu.Unlock()
		t.Fatalf("StartProcess wire request_ids = %q, want [\"\", \"caller-explicit-id\"]", gotReqID)
	}
	for i, md := range gotMD {
		if got := md.Get(ate.TargetActorHeader); len(got) != 1 || got[0] != "ate-env/default-env" {
			t.Errorf("call %d %s = %q, want [\"ate-env/default-env\"]", i, ate.TargetActorHeader, got)
		}
		for k := range md {
			if strings.HasPrefix(k, "x-ate-") {
				t.Errorf("call %d sent unexpected header %q = %q with flags off", i, k, md.Get(k))
			}
		}
	}
	mu.Unlock()

	// 3. With ReactiveWake off, guest Unavailable, CreateEnvironment,
	// SuspendEnvironment, and DeleteEnvironment must not populate boundedAwakeCache.
	stub.onGet = func(_ context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
		if req.GetProcessId() == "ate-env-wake-probe" {
			return nil, status.Error(codes.NotFound, "probe ok")
		}
		return nil, status.Error(codes.Unavailable, "guest unavailable")
	}
	if _, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "p-1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("GetProcess with ReactiveWake=false on guest Unavailable = %v, want codes.Unavailable", err)
	}
	if got := te.srv.AwakeCacheLen(); got != 0 {
		t.Fatalf("AwakeCacheLen after guest Unavailable with ReactiveWake=false = %d, want 0", got)
	}
	stub.onGet = nil
	if _, err := te.envClient.SuspendEnvironment(ctx, &ateenvv1alpha.SuspendEnvironmentRequest{Id: "default-env"}); err != nil {
		t.Fatalf("SuspendEnvironment: %v", err)
	}
	if _, err := te.envClient.DeleteEnvironment(ctx, &ateenvv1alpha.DeleteEnvironmentRequest{Id: "default-env"}); err != nil {
		t.Fatalf("DeleteEnvironment: %v", err)
	}
	if got := te.srv.AwakeCacheLen(); got != 0 {
		t.Fatalf("AwakeCacheLen with ReactiveWake=false = %d, want 0", got)
	}

	// 4. Server.Close rejects subsequent per-call (RouterPoolSize == 0) sessions with Unavailable.
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "default-env"}); err != nil {
		t.Fatalf("re-CreateEnvironment: %v", err)
	}
	te.srv.Close()
	if _, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "p-1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("GetProcess after Close() with RouterPoolSize=0 = %v, want codes.Unavailable", err)
	}
}

// TestPooledRouterAndReactiveWake checks WF-9, WF-10, and WF-12.
func TestPooledRouterAndReactiveWake(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 4
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)
	installWakeOnRouterRequest(te)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "pool-test"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	guestSvc, cleanup, err := guest.NewServer(guest.Config{
		Workspace:        t.TempDir(),
		EnableProcess:    true,
		EnableFileSystem: true,
	})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanup)
	te.router.Register("pool-test", guestSvc)

	envCtx := metadata.AppendToOutgoingContext(ctx,
		"x-env-id", "pool-test",
		"x-env-atespace", "ate-env",
	)

	// 1. Concurrent unary calls across the 4-connection pool with ReactiveWake enabled.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			proc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
				Command: []string{"true"},
			})
			if err != nil {
				t.Errorf("iter %d StartProcess: %v", i, err)
				return
			}
			exit := waitProcess(t, te.procClient, envCtx, proc.GetProcessId())
			if exit.GetExitCode() != 0 {
				t.Errorf("iter %d exit code = %d, want 0", i, exit.GetExitCode())
			}
		}(i)
	}
	wg.Wait()

	// Verify that router accepted connections do not exceed RouterPoolSize.
	if conns := te.router.AcceptedConns(); conns == 0 || conns > int64(te.srv.RouterPoolSize) {
		t.Fatalf("router accepted %d connections, want > 0 and <= %d (RouterPoolSize)", conns, te.srv.RouterPoolSize)
	}

	// 2. SuspendEnvironment must evict awakeEnvs. SignalProcess does not replay on
	// fast-path Unavailable, so if SuspendEnvironment left a stale awakeEnvs entry,
	// SignalProcess against the suspended actor would skip ensureAwake and fail
	// with Unavailable instead of waking the actor first.
	sleepProc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sleep", "60"},
	})
	if err != nil {
		t.Fatalf("StartProcess sleep: %v", err)
	}
	if _, err := te.envClient.SuspendEnvironment(ctx, &ateenvv1alpha.SuspendEnvironmentRequest{Id: "pool-test"}); err != nil {
		t.Fatalf("SuspendEnvironment: %v", err)
	}
	if _, err := te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: sleepProc.GetProcessId(),
		Signal:    ateenvv1alpha.Signal_SIGNAL_TERM,
	}); err != nil {
		t.Fatalf("post-suspend SignalProcess failed (SuspendEnvironment must evict awakeEnvs): %v", err)
	}
	_ = waitProcess(t, te.procClient, envCtx, sleepProc.GetProcessId())

	// 3. DeleteEnvironment + recreating under the same ID (starting suspended) also evicts awakeEnvs.
	sleepProc2, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sleep", "60"},
	})
	if err != nil {
		t.Fatalf("StartProcess sleep2: %v", err)
	}
	if _, err := te.envClient.DeleteEnvironment(ctx, &ateenvv1alpha.DeleteEnvironmentRequest{Id: "pool-test"}); err != nil {
		t.Fatalf("DeleteEnvironment: %v", err)
	}
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "pool-test"}); err != nil {
		t.Fatalf("re-CreateEnvironment: %v", err)
	}
	te.control.MutateActor("pool-test", func(a *ateapipb.Actor) {
		a.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	})
	if _, err := te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: sleepProc2.GetProcessId(),
		Signal:    ateenvv1alpha.Signal_SIGNAL_TERM,
	}); err != nil {
		t.Fatalf("post-recreate SignalProcess failed (DeleteEnvironment/CreateEnvironment must evict awakeEnvs): %v", err)
	}
	_ = waitProcess(t, te.procClient, envCtx, sleepProc2.GetProcessId())

	// 4. Verify that Server.Close closes all pooled connections and refuses to dial a new pool afterward.
	pooledConns := te.srv.RouterConns()
	if len(pooledConns) != te.srv.RouterPoolSize {
		t.Fatalf("len(RouterConns) = %d, want %d", len(pooledConns), te.srv.RouterPoolSize)
	}
	for i, c := range pooledConns {
		if c.GetState() == connectivity.Shutdown {
			t.Errorf("pooled conn %d is unexpectedly in Shutdown before Close()", i)
		}
	}
	te.srv.Close()
	for i, c := range pooledConns {
		if c.GetState() != connectivity.Shutdown {
			t.Errorf("pooled conn %d state = %v, want connectivity.Shutdown after Close()", i, c.GetState())
		}
	}
	_, closeErr := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: sleepProc2.GetProcessId()})
	if status.Code(closeErr) != codes.Unavailable {
		t.Fatalf("GetProcess after Server.Close() = %v, want codes.Unavailable", closeErr)
	}
	if got := len(te.srv.RouterConns()); got != 0 {
		t.Fatalf("len(RouterConns) after post-Close call = %d, want 0 (must not redial pool)", got)
	}
}

type wrappedProcessService struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	realSvc    ateenvv1alpha.ProcessServiceServer
	execCount  *atomic.Int32
	failFirst  *atomic.Bool
	seenReqIDs sync.Map
}

func (w *wrappedProcessService) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	if req.GetRequestId() != "" {
		w.seenReqIDs.Store(req.GetRequestId(), struct{}{})
	}
	w.execCount.Add(1)
	if w.failFirst.CompareAndSwap(true, false) {
		// Simulate guest executing StartProcess and caching the idempotency record,
		// while the transport drops mid-response before the client receives the reply.
		_, _ = w.realSvc.StartProcess(ctx, req)
		return nil, status.Error(codes.Unavailable, "transport is closing")
	}
	return w.realSvc.StartProcess(ctx, req)
}

func (w *wrappedProcessService) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	return w.realSvc.GetProcess(ctx, req)
}

func (w *wrappedProcessService) SignalProcess(ctx context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	w.execCount.Add(1)
	if w.failFirst.CompareAndSwap(true, false) {
		return nil, status.Error(codes.Unavailable, "transport is closing")
	}
	return w.realSvc.SignalProcess(ctx, req)
}

func (w *wrappedProcessService) ResizeProcess(ctx context.Context, req *ateenvv1alpha.ResizeProcessRequest) (*ateenvv1alpha.ResizeProcessResponse, error) {
	return w.realSvc.ResizeProcess(ctx, req)
}

func (w *wrappedProcessService) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, srv grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	return w.realSvc.StreamProcessOutput(req, srv)
}

func (w *wrappedProcessService) WriteProcessInput(srv grpc.ClientStreamingServer[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]) error {
	return w.realSvc.WriteProcessInput(srv)
}

// TestReactiveWakeDoesNotReplayExecutedStartProcess checks WF-11 and WF-13.
func TestReactiveWakeDoesNotReplayExecutedStartProcess(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "env-1"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	workspace := t.TempDir()
	var startedProcs atomic.Int32
	tracker, err := process.NewTracker(process.TrackerConfig{
		Workspace: workspace,
		OnExit: func(time.Duration, string) {
			startedProcs.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("NewTracker: %v", err)
	}
	t.Cleanup(func() { tracker.Close() })
	realProcSvc := process.NewService(tracker)

	var execCount atomic.Int32
	var failFirst atomic.Bool
	wrapped := &wrappedProcessService{
		realSvc:   realProcSvc,
		execCount: &execCount,
		failFirst: &failFirst,
	}

	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, wrapped)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register("env-1", grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx,
		"x-env-id", "env-1",
		"x-env-atespace", "ate-env",
	)

	// Warm the environment so it is in awakeEnvs with start-request-id recorded.
	_, _ = te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "ate-env-wake-probe"})

	// Case A: Mid-flight transport drop on StartProcess.
	// Attempt 1 runs in the guest and records the idempotency entry, then drops
	// with Unavailable. Attempt 2 wakes and retries with the same request_id,
	// returning the existing process without launching a second child process.
	failFirst.Store(true)
	execCount.Store(0)
	runsFile := filepath.Join(workspace, "runs.txt")

	proc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "printf 'run\\n' >> " + runsFile},
	})
	if err != nil {
		t.Fatalf("StartProcess failed: %v", err)
	}
	if proc.GetProcessId() == "" {
		t.Fatalf("StartProcess returned empty ProcessId")
	}

	exit := waitProcess(t, te.procClient, envCtx, proc.GetProcessId())
	if exit.GetExitCode() != 0 {
		t.Fatalf("exit code = %d, want 0", exit.GetExitCode())
	}
	if got := execCount.Load(); got != 2 {
		t.Fatalf("StartProcess RPC invocations = %d, want 2 (attempt 1 + retry)", got)
	}
	if got := startedProcs.Load(); got != 1 {
		t.Fatalf("tracker started %d processes, want strictly 1 (guest request_id deduplication)", got)
	}
	data, err := os.ReadFile(runsFile)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", runsFile, err)
	}
	if string(data) != "run\n" {
		t.Fatalf("command side effect = %q, want single execution %q", string(data), "run\n")
	}

	// Case B: SignalProcess carries no request_id, so a fast-path Unavailable
	// evicts awakeEnvs and returns Unavailable without replaying.
	failFirst.Store(true)
	execCount.Store(0)

	_, sigErr := te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: proc.GetProcessId(),
		Signal:    ateenvv1alpha.Signal_SIGNAL_TERM,
	})
	if status.Code(sigErr) != codes.Unavailable {
		t.Fatalf("SignalProcess status = %v (%v), want Unavailable", status.Code(sigErr), sigErr)
	}
	if count := execCount.Load(); count != 1 {
		t.Fatalf("SignalProcess executed %d times, want strictly 1", count)
	}
}

type inspectingGuestServer struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	onGet   func(context.Context, *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error)
	onStart func(context.Context, *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error)
}

func (s *inspectingGuestServer) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	if s.onGet != nil {
		return s.onGet(ctx, req)
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func (s *inspectingGuestServer) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	return s.onStart(ctx, req)
}

// TestReactiveWakeOldGuestWithoutCapability checks WF-11, WF-12, and WF-13
// (including capability eviction across same-replica DeleteEnvironment and recreate).
func TestReactiveWakeOldGuestWithoutCapability(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "env-old-guest"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	var (
		execCount atomic.Int32
		gotReqID  atomic.Value
	)
	oldGuest := &inspectingGuestServer{
		onStart: func(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
			gotReqID.Store(req.GetRequestId())
			if execCount.Add(1) == 1 {
				return nil, status.Error(codes.Unavailable, "transport reset after old guest received request")
			}
			return &ateenvv1alpha.Process{ProcessId: "unexpected-replay"}, nil
		},
	}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, oldGuest)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register("env-old-guest", grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "env-old-guest", "x-env-atespace", "ate-env")

	// Warm env-old-guest in awakeEnvs; oldGuest sends no x-ate-env-guest-caps trailer.
	_, _ = te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "ate-env-wake-probe"})

	_, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"true"},
		RequestId: "caller-id-for-old-guest",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("StartProcess against old guest = %v, want Unavailable (no replay without start-request-id)", err)
	}
	if got := execCount.Load(); got != 1 {
		t.Fatalf("StartProcess invocations on old guest = %d, want strictly 1", got)
	}
	if id, _ := gotReqID.Load().(string); id != "" {
		t.Fatalf("old guest received request_id = %q, want empty (cleared without start-request-id capability)", id)
	}
	if te.srv.IsAwakeCached("ate-env/env-old-guest") {
		t.Fatal("expected env-old-guest to be evicted from awake cache after fast-path Unavailable on old guest StartProcess")
	}

	// Same-replica DeleteEnvironment + recreate with an older guest after a
	// modern guest advertised start-request-id must clear cached capabilities so
	// StartProcess is not replayed against the older guest.
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "env-recreate-old"}); err != nil {
		t.Fatalf("CreateEnvironment(env-recreate-old): %v", err)
	}
	modernGuest, cleanup, err := guest.NewServer(guest.Config{
		Workspace:     t.TempDir(),
		EnableProcess: true,
	})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanup)
	te.router.Register("env-recreate-old", modernGuest)

	recreateCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "env-recreate-old", "x-env-atespace", "ate-env")
	_, _ = te.procClient.GetProcess(recreateCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "ate-env-wake-probe"})

	if _, err := te.envClient.DeleteEnvironment(ctx, &ateenvv1alpha.DeleteEnvironmentRequest{Id: "env-recreate-old"}); err != nil {
		t.Fatalf("DeleteEnvironment(env-recreate-old): %v", err)
	}
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "env-recreate-old"}); err != nil {
		t.Fatalf("re-CreateEnvironment(env-recreate-old): %v", err)
	}
	execCount.Store(0)
	gotReqID.Store("unset")
	te.router.Register("env-recreate-old", grpcSrv)
	_, _ = te.procClient.GetProcess(recreateCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "ate-env-wake-probe"})

	_, err = te.procClient.StartProcess(recreateCtx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"true"},
		RequestId: "caller-id-after-recreate",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("StartProcess after recreate with old guest = %v, want Unavailable", err)
	}
	if got := execCount.Load(); got != 1 {
		t.Fatalf("StartProcess invocations after recreate with old guest = %d, want strictly 1 (Remove must clear capabilities)", got)
	}
	if id, _ := gotReqID.Load().(string); id != "" {
		t.Fatalf("recreated old guest received request_id = %q, want empty", id)
	}
}

// TestReactiveWakeRetriesIdempotentProcessAndFileCalls checks WF-11.
func TestReactiveWakeRetriesIdempotentProcessAndFileCalls(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)
	installWakeOnRouterRequest(te)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "env-retry"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	guestSvc, cleanup, err := guest.NewServer(guest.Config{
		Workspace:        t.TempDir(),
		EnableProcess:    true,
		EnableFileSystem: true,
	})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanup)
	te.router.Register("env-retry", guestSvc)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "env-retry", "x-env-atespace", "ate-env")

	// Seed a file and a running process, and warm env-retry in awakeEnvs.
	writeStream, err := te.fsClient.WriteFile(envCtx)
	if err != nil {
		t.Fatalf("WriteFile open: %v", err)
	}
	if err := writeStream.Send(&ateenvv1alpha.WriteFileRequest{
		Path:  "hello.txt",
		Mode:  0644,
		Chunk: []byte("hello-after-wake"),
	}); err != nil {
		t.Fatalf("WriteFile send: %v", err)
	}
	if _, err := writeStream.CloseAndRecv(); err != nil {
		t.Fatalf("WriteFile close: %v", err)
	}

	nonPTYProc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sleep", "60"},
	})
	if err != nil {
		t.Fatalf("StartProcess non-PTY sleep: %v", err)
	}

	var ptyProc *ateenvv1alpha.Process
	if runtime.GOOS == "linux" {
		var err error
		ptyProc, err = te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
			Command: []string{"sh", "-c", "printf 'pty-ready\\n'; sleep 60"},
			Tty:     &ateenvv1alpha.TTY{Rows: 24, Cols: 80},
		})
		if err != nil {
			t.Fatalf("StartProcess PTY: %v", err)
		}
	}

	echoProc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"sh", "-c", "printf 'stream-ok'"},
	})
	if err != nil {
		t.Fatalf("StartProcess echo: %v", err)
	}
	_ = waitProcess(t, te.procClient, envCtx, echoProc.GetProcessId())

	// suspendOutOfBand suspends the actor directly in fakecontrol without going
	// through ate-env-api, leaving env-retry cached as awake in awakeEnvs.
	suspendOutOfBand := func() {
		t.Helper()
		// Ensure env-retry is cached in awakeEnvs before the out-of-band suspend.
		if _, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: echoProc.GetProcessId()}); err != nil {
			t.Fatalf("warm GetProcess: %v", err)
		}
		if _, err := te.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   &ateapipb.ObjectRef{Atespace: "ate-env", Name: "env-retry"},
			Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
		}); err != nil {
			t.Fatalf("out-of-band SuspendActor: %v", err)
		}
	}

	// 0. GetProcess second attempt after fast-path Unavailable.
	suspendOutOfBand()
	gotProc, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{
		ProcessId: echoProc.GetProcessId(),
	})
	if err != nil {
		t.Fatalf("GetProcess after out-of-band suspend: %v", err)
	}
	if gotProc.GetProcessId() != echoProc.GetProcessId() {
		t.Fatalf("GetProcess id = %q, want %q", gotProc.GetProcessId(), echoProc.GetProcessId())
	}

	// 1a. ResizeProcess second attempt after fast-path Unavailable (all platforms:
	// without the second attempt the suspended router returns Unavailable; after
	// waking, the live guest rejects resizing a non-PTY process with FailedPrecondition).
	suspendOutOfBand()
	_, err = te.procClient.ResizeProcess(envCtx, &ateenvv1alpha.ResizeProcessRequest{
		ProcessId: nonPTYProc.GetProcessId(),
		Rows:      40,
		Cols:      120,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResizeProcess on non-PTY process after out-of-band suspend = %v, want codes.FailedPrecondition from woken guest", err)
	}

	// 1b. ResizeProcess second attempt on a live PTY (Linux only).
	if runtime.GOOS == "linux" && ptyProc != nil {
		suspendOutOfBand()
		if _, err := te.procClient.ResizeProcess(envCtx, &ateenvv1alpha.ResizeProcessRequest{
			ProcessId: ptyProc.GetProcessId(),
			Rows:      40,
			Cols:      120,
		}); err != nil {
			t.Fatalf("ResizeProcess after out-of-band suspend: %v", err)
		}
	}

	// 2. StreamProcessOutput second attempt after fast-path Unavailable.
	suspendOutOfBand()
	outStream, err := te.procClient.StreamProcessOutput(envCtx, &ateenvv1alpha.StreamProcessOutputRequest{
		ProcessId: echoProc.GetProcessId(),
		Follow:    true,
	})
	if err != nil {
		t.Fatalf("StreamProcessOutput after out-of-band suspend: %v", err)
	}
	var gotOut strings.Builder
	var gotExit *ateenvv1alpha.Process
	for {
		msg, err := outStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("StreamProcessOutput Recv after out-of-band suspend: %v", err)
		}
		if b := msg.GetStdout(); len(b) > 0 {
			gotOut.Write(b)
		}
		if ex := msg.GetExit(); ex != nil {
			gotExit = ex
		}
	}
	if gotOut.String() != "stream-ok" || gotExit == nil || gotExit.GetExitCode() != 0 {
		t.Fatalf("StreamProcessOutput got stdout=%q exit=%v, want \"stream-ok\" and exit 0", gotOut.String(), gotExit)
	}

	// 3. ReadFile second attempt after fast-path Unavailable.
	suspendOutOfBand()
	rfStream, err := te.fsClient.ReadFile(envCtx, &ateenvv1alpha.ReadFileRequest{Path: "hello.txt"})
	if err != nil {
		t.Fatalf("ReadFile after out-of-band suspend: %v", err)
	}
	var fileBytes []byte
	for {
		chunk, err := rfStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadFile Recv after out-of-band suspend: %v", err)
		}
		fileBytes = append(fileBytes, chunk.GetChunk()...)
	}
	if string(fileBytes) != "hello-after-wake" {
		t.Fatalf("ReadFile after out-of-band suspend = %q, want %q", string(fileBytes), "hello-after-wake")
	}

	// 4. SignalProcess non-idempotent fast-path Unavailable must evict awake cache and NOT retry.
	suspendOutOfBand()
	if !te.srv.IsAwakeCached("ate-env/env-retry") {
		t.Fatal("expected env-retry to be cached awake before SignalProcess fast-path attempt")
	}
	_, err = te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: echoProc.GetProcessId(),
		Signal:    ateenvv1alpha.Signal_SIGNAL_KILL,
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("SignalProcess after out-of-band suspend returned %v, want codes.Unavailable", err)
	}
	if te.srv.IsAwakeCached("ate-env/env-retry") {
		t.Fatal("expected env-retry to be evicted from awake cache after fast-path Unavailable on SignalProcess")
	}

	// Clean up long-running processes.
	_, _ = te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: nonPTYProc.GetProcessId(),
		Signal:    ateenvv1alpha.Signal_SIGNAL_KILL,
	})
	_ = waitProcess(t, te.procClient, envCtx, nonPTYProc.GetProcessId())
	if ptyProc != nil {
		_, _ = te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
			ProcessId: ptyProc.GetProcessId(),
			Signal:    ateenvv1alpha.Signal_SIGNAL_KILL,
		})
		_ = waitProcess(t, te.procClient, envCtx, ptyProc.GetProcessId())
	}
}

// TestFirstStartProcessPreservesCallerRequestIDWithReactiveWake checks WF-13.
func TestFirstStartProcessPreservesCallerRequestIDWithReactiveWake(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "cold-env"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	tracker, err := process.NewTracker(process.TrackerConfig{Workspace: t.TempDir()})
	if err != nil {
		t.Fatalf("NewTracker: %v", err)
	}
	t.Cleanup(func() { tracker.Close() })
	realProcSvc := process.NewService(tracker)

	var execCount atomic.Int32
	wrapped := &wrappedProcessService{
		realSvc:   realProcSvc,
		execCount: &execCount,
		failFirst: &atomic.Bool{},
	}

	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, wrapped)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register("cold-env", grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx,
		"x-env-id", "cold-env",
		"x-env-atespace", "ate-env",
	)

	// Cold first call under ReactiveWake runs ensureAwake, learns start-request-id
	// from GetProcess's response trailer, and preserves the caller's request_id.
	explicitID := "client-req-uuid-1234"
	proc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command:   []string{"true"},
		RequestId: explicitID,
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	if proc.GetProcessId() == "" {
		t.Fatalf("StartProcess returned empty process id")
	}
	if _, ok := wrapped.seenReqIDs.Load(explicitID); !ok {
		t.Fatalf("expected guest to see request_id %q on cold first call, got missing", explicitID)
	}
}

// TestWakeForcesLiveProbeWhenReactiveWakeCached checks WF-10 and WF-12.
func TestWakeForcesLiveProbeWhenReactiveWakeCached(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)
	installWakeOnRouterRequest(te)

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "wake-cached"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	guestSvc, cleanup, err := guest.NewServer(guest.Config{
		Workspace:     t.TempDir(),
		EnableProcess: true,
	})
	if err != nil {
		t.Fatalf("guest.NewServer: %v", err)
	}
	t.Cleanup(cleanup)
	te.router.Register("wake-cached", guestSvc)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "wake-cached", "x-env-atespace", "ate-env")
	proc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
		Command: []string{"true"},
	})
	if err != nil {
		t.Fatalf("StartProcess: %v", err)
	}
	_ = waitProcess(t, te.procClient, envCtx, proc.GetProcessId())

	if !te.srv.IsAwakeCached("ate-env/wake-cached") {
		t.Fatal("expected wake-cached to be in awake cache")
	}

	// Out-of-band suspend leaves wake-cached in awakeEnvs, but Server.Wake
	// (portproxy X-Env-Wake-Wait) must still probe and wake the guest.
	if _, err := te.control.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor:   &ateapipb.ObjectRef{Atespace: "ate-env", Name: "wake-cached"},
		Options: &ateapipb.SuspendOptions{Mode: ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("out-of-band SuspendActor: %v", err)
	}

	if err := te.srv.Wake(ctx, "ate-env", "wake-cached"); err != nil {
		t.Fatalf("Wake after out-of-band suspend: %v", err)
	}
	resp, err := te.envClient.GetEnvironment(ctx, &ateenvv1alpha.GetEnvironmentRequest{Id: "wake-cached"})
	if err != nil {
		t.Fatalf("GetEnvironment: %v", err)
	}
	if resp.GetEnvironment().GetStatus() != ateenvv1alpha.EnvironmentStatus_ENVIRONMENT_STATUS_RUNNING {
		t.Fatalf("status after Wake = %v, want RUNNING", resp.GetEnvironment().GetStatus())
	}

	// If ensureAwake fails during a forced Wake (for example, the environment
	// crashed out-of-band while cached awake), guestSession must evict the entry.
	if !te.srv.IsAwakeCached("ate-env/wake-cached") {
		t.Fatal("expected wake-cached to be cached awake after Wake")
	}
	te.control.MutateActor("wake-cached", func(a *ateapipb.Actor) {
		a.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
		a.Status.Crash = &ateapipb.ActorCrash{Message: "container exited"}
	})
	if err := te.srv.Wake(ctx, "ate-env", "wake-cached"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Wake on crashed cached env = %v, want codes.FailedPrecondition", err)
	}
	if te.srv.IsAwakeCached("ate-env/wake-cached") {
		t.Fatal("expected failed Wake to evict wake-cached from awake cache")
	}
}

// TestReactiveWakeCrashedCachedEnvReturnsFailedPrecondition checks WF-11 and WF-12.
func TestReactiveWakeCrashedCachedEnvReturnsFailedPrecondition(t *testing.T) {
	for _, poolSize := range []int{0, 2} {
		name := "pool=0"
		if poolSize > 0 {
			name = "pool=2"
		}
		t.Run(name, func(t *testing.T) {
			te := newFullTestEnv(t)
			te.srv.RouterPoolSize = poolSize
			te.srv.ReactiveWake = true
			t.Cleanup(te.srv.Close)
			te.router.Running = func(id string) bool {
				return te.control.Status(id) == ateapipb.ActorState_ACTOR_STATE_RUNNING
			}

			ctx := context.Background()
			guestSvc, cleanup, err := guest.NewServer(guest.Config{
				Workspace:        t.TempDir(),
				EnableProcess:    true,
				EnableFileSystem: true,
			})
			if err != nil {
				t.Fatalf("guest.NewServer: %v", err)
			}
			t.Cleanup(cleanup)

			setupCachedThenCrashed := func(envID string) (context.Context, string) {
				t.Helper()
				if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: envID}); err != nil {
					t.Fatalf("CreateEnvironment(%s): %v", envID, err)
				}
				te.router.Register(envID, guestSvc)
				envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", "ate-env")

				ws, err := te.fsClient.WriteFile(envCtx)
				if err != nil {
					t.Fatalf("WriteFile open: %v", err)
				}
				if err := ws.Send(&ateenvv1alpha.WriteFileRequest{Path: envID + ".txt", Mode: 0644, Chunk: []byte("ok")}); err != nil {
					t.Fatalf("WriteFile send: %v", err)
				}
				if _, err := ws.CloseAndRecv(); err != nil {
					t.Fatalf("WriteFile close: %v", err)
				}

				proc, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{
					Command: []string{"sh", "-c", "printf 'done'"},
				})
				if err != nil {
					t.Fatalf("StartProcess: %v", err)
				}
				_ = waitProcess(t, te.procClient, envCtx, proc.GetProcessId())

				if !te.srv.IsAwakeCached("ate-env/" + envID) {
					t.Fatalf("expected %s to be cached awake before crash", envID)
				}

				te.control.MutateActor(envID, func(a *ateapipb.Actor) {
					a.Status.State = ateapipb.ActorState_ACTOR_STATE_CRASHED
					a.Status.Crash = &ateapipb.ActorCrash{Message: "container exited"}
				})
				return envCtx, proc.GetProcessId()
			}

			// 1. ReadFile on a cached, crashed environment must return FailedPrecondition without panicking on nil closeFn.
			rfCtx, _ := setupCachedThenCrashed("crash-read")
			rfStream, err := te.fsClient.ReadFile(rfCtx, &ateenvv1alpha.ReadFileRequest{Path: "crash-read.txt"})
			if err == nil {
				_, err = rfStream.Recv()
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("ReadFile on crashed cached env = %v, want codes.FailedPrecondition", err)
			}
			if te.srv.IsAwakeCached("ate-env/crash-read") {
				t.Fatal("expected crash-read to be evicted from awake cache")
			}

			// 2. StreamProcessOutput on a cached, crashed environment must return FailedPrecondition without panicking on nil closeFn.
			spCtx, procID := setupCachedThenCrashed("crash-stream")
			spStream, err := te.procClient.StreamProcessOutput(spCtx, &ateenvv1alpha.StreamProcessOutputRequest{
				ProcessId: procID,
				Follow:    true,
			})
			if err == nil {
				_, err = spStream.Recv()
			}
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("StreamProcessOutput on crashed cached env = %v, want codes.FailedPrecondition", err)
			}
			if te.srv.IsAwakeCached("ate-env/crash-stream") {
				t.Fatal("expected crash-stream to be evicted from awake cache")
			}
		})
	}
}

type probeBlockingProcessService struct {
	ateenvv1alpha.ProcessServiceServer
	blockNextProbe atomic.Bool
	probeEntered   chan struct{}
	releaseProbe   chan struct{}
}

func (p *probeBlockingProcessService) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	if req.GetProcessId() == "ate-env-wake-probe" && p.blockNextProbe.CompareAndSwap(true, false) {
		close(p.probeEntered)
		select {
		case <-p.releaseProbe:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.ProcessServiceServer.GetProcess(ctx, req)
}

// TestReactiveWakeProbeBlockedAcrossSuspend checks WF-12.
func TestReactiveWakeProbeBlockedAcrossSuspend(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	// Simulate an asynchronous resume: when SUSPENDED, the first router attempt
	// starts resuming while still returning 503 Unavailable, and the second
	// attempt observes RUNNING.
	var resumeHits atomic.Int32
	te.router.Running = func(id string) bool {
		switch te.control.Status(id) {
		case ateapipb.ActorState_ACTOR_STATE_RUNNING:
			return true
		case ateapipb.ActorState_ACTOR_STATE_SUSPENDED:
			if resumeHits.Add(1) >= 2 {
				_, _ = te.control.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
					Actor: &ateapipb.ObjectRef{Atespace: "ate-env", Name: id},
				})
				return true
			}
		}
		return false
	}

	ctx := context.Background()
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: "suspend-race"}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	tracker, err := process.NewTracker(process.TrackerConfig{Workspace: t.TempDir()})
	if err != nil {
		t.Fatalf("NewTracker: %v", err)
	}
	t.Cleanup(func() { tracker.Close() })
	sleepState, err := tracker.Start(process.StartOptions{Command: []string{"sleep", "60"}})
	if err != nil {
		t.Fatalf("tracker.Start: %v", err)
	}

	blockingSvc := &probeBlockingProcessService{
		ProcessServiceServer: process.NewService(tracker),
		probeEntered:         make(chan struct{}),
		releaseProbe:         make(chan struct{}),
	}
	blockingSvc.blockNextProbe.Store(true)

	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, blockingSvc)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register("suspend-race", grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", "suspend-race", "x-env-atespace", "ate-env")

	// 1. Start Server.Wake while suspend-race is not cached; its ensureAwake probe blocks in GetProcess.
	// Using Wake (which returns directly after guestSession/AddIfGen without a
	// forwarded guest RPC) isolates AddIfGen's generation check from cold-RPC
	// Unavailable eviction.
	wakeDone := make(chan error, 1)
	go func() {
		wakeDone <- te.srv.Wake(ctx, "ate-env", "suspend-race")
	}()

	<-blockingSvc.probeEntered

	// 2. Suspend the environment while ensureAwake is in flight.
	if _, err := te.envClient.SuspendEnvironment(ctx, &ateenvv1alpha.SuspendEnvironmentRequest{Id: "suspend-race"}); err != nil {
		t.Fatalf("SuspendEnvironment: %v", err)
	}

	// 3. Release the wake probe so ensureAwake returns nil.
	close(blockingSvc.releaseProbe)
	if werr := <-wakeDone; werr != nil {
		t.Fatalf("concurrent Wake across SuspendEnvironment = %v, want nil", werr)
	}

	// Because SuspendEnvironment incremented the key's generation during ensureAwake,
	// AddIfGen must not mark suspend-race awake.
	if te.srv.IsAwakeCached("ate-env/suspend-race") {
		t.Fatal("suspend-race was marked awake by an ensureAwake that overlapped SuspendEnvironment")
	}

	// 4. The next SignalProcess must wake the suspended environment via ensureAwake instead of failing fast with Unavailable.
	if _, err := te.procClient.SignalProcess(envCtx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: sleepState.ProcessID,
		Signal:    ateenvv1alpha.Signal_SIGNAL_TERM,
	}); err != nil {
		t.Fatalf("SignalProcess after concurrent SuspendEnvironment failed: %v", err)
	}
	_ = waitProcess(t, te.procClient, envCtx, sleepState.ProcessID)
}

type coldUnavailableGuestServer struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	ateenvv1alpha.UnimplementedFileSystemServiceServer
	calls atomic.Int32
}

func (g *coldUnavailableGuestServer) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	_ = grpc.SetTrailer(ctx, metadata.Pairs("x-ate-env-guest-caps", "start-request-id"))
	if req.GetProcessId() == "ate-env-wake-probe" {
		return nil, status.Error(codes.NotFound, "probe ok")
	}
	g.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "actor suspended right after wake probe")
}

func (g *coldUnavailableGuestServer) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	g.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "actor suspended right after wake probe")
}

func (g *coldUnavailableGuestServer) ResizeProcess(ctx context.Context, req *ateenvv1alpha.ResizeProcessRequest) (*ateenvv1alpha.ResizeProcessResponse, error) {
	g.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "actor suspended right after wake probe")
}

func (g *coldUnavailableGuestServer) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	g.calls.Add(1)
	return status.Error(codes.Unavailable, "actor suspended right after wake probe")
}

func (g *coldUnavailableGuestServer) ReadFile(req *ateenvv1alpha.ReadFileRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ReadFileResponse]) error {
	g.calls.Add(1)
	return status.Error(codes.Unavailable, "actor suspended right after wake probe")
}

// TestReactiveWakeEvictsCacheOnColdUnavailable checks WF-11 and WF-12: when a
// cold guestSession (fastPath == false) finishes ensureAwake and caches the
// environment in awakeEnvs, an Unavailable from the forwarded guest RPC must
// evict the cache entry without replaying the call.
func TestReactiveWakeEvictsCacheOnColdUnavailable(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	stub := &coldUnavailableGuestServer{}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, stub)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcSrv, stub)
	t.Cleanup(grpcSrv.Stop)

	cases := []struct {
		name string
		call func(envCtx context.Context) error
	}{
		{
			name: "GetProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "p-1"})
				return err
			},
		},
		{
			name: "StartProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
				return err
			},
		},
		{
			name: "ResizeProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.ResizeProcess(envCtx, &ateenvv1alpha.ResizeProcessRequest{ProcessId: "p-1", Rows: 24, Cols: 80})
				return err
			},
		},
		{
			name: "StreamProcessOutput",
			call: func(envCtx context.Context) error {
				stream, err := te.procClient.StreamProcessOutput(envCtx, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: "p-1"})
				if err != nil {
					return err
				}
				_, err = stream.Recv()
				return err
			},
		},
		{
			name: "ReadFile",
			call: func(envCtx context.Context) error {
				stream, err := te.fsClient.ReadFile(envCtx, &ateenvv1alpha.ReadFileRequest{Path: "a.txt"})
				if err != nil {
					return err
				}
				_, err = stream.Recv()
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			envID := "cold-unavail-" + strings.ToLower(tc.name)
			if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: envID}); err != nil {
				t.Fatalf("CreateEnvironment(%s): %v", envID, err)
			}
			te.router.Register(envID, grpcSrv)
			envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", "ate-env")

			stub.calls.Store(0)
			err := tc.call(envCtx)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("%s = %v, want codes.Unavailable", tc.name, err)
			}
			if got := stub.calls.Load(); got != 1 {
				t.Fatalf("%s forwarded invocations = %d, want 1 (cold call must not retry)", tc.name, got)
			}
			if te.srv.IsAwakeCached("ate-env/" + envID) {
				t.Fatalf("%s left %s cached awake after cold Unavailable", tc.name, envID)
			}
		})
	}
}

// TestReactiveWakeEvictsCacheOnWriteFileMidStreamUnavailable checks that when a guest drops
// mid-stream with Unavailable during WriteFile, WriteFile calls CloseAndRecv to surface
// codes.Unavailable (instead of code = Unknown desc = EOF) and evicts the environment from
// awakeEnvs.
func TestReactiveWakeEvictsCacheOnWriteFileMidStreamUnavailable(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	envID := "wf-midstream-unavail"
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: envID}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	stub := &midStreamUnavailableGuestServer{}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, stub)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcSrv, stub)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register(envID, grpcSrv)

	envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", "ate-env")

	// 1. Open WriteFile stream and send the initial chunk (which also triggers ensureAwake and caches the environment).
	ws, err := te.fsClient.WriteFile(envCtx)
	if err != nil {
		t.Fatalf("WriteFile open: %v", err)
	}

	if err := ws.Send(&ateenvv1alpha.WriteFileRequest{
		Path:  "stream.txt",
		Mode:  0644,
		Chunk: []byte("first-chunk"),
	}); err != nil {
		t.Fatalf("WriteFile send chunk 1: %v", err)
	}

	// Wait until stub has received the first chunk and aborted the stream.
	for i := 0; i < 50 && stub.receivedChunks.Load() == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if stub.receivedChunks.Load() == 0 {
		t.Fatal("stub never received first chunk")
	}

	if !te.srv.IsAwakeCached("ate-env/" + envID) {
		t.Fatalf("expected %s to be cached awake after initial ensureAwake", envID)
	}

	// 2. Send the second chunk. The guest has closed the stream with Unavailable, so clientStream.Send returns io.EOF.
	// WriteFile must call CloseAndRecv, evict the environment from awakeEnvs, and surface codes.Unavailable.
	_ = ws.Send(&ateenvv1alpha.WriteFileRequest{
		Chunk: []byte("second-chunk"),
	})

	_, err = ws.CloseAndRecv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("WriteFile CloseAndRecv = %v, want codes.Unavailable", err)
	}

	// 3. awakeEnvs must be evicted.
	if te.srv.IsAwakeCached("ate-env/" + envID) {
		t.Fatalf("WriteFile left %s cached awake after mid-stream Unavailable", envID)
	}
}

type midStreamUnavailableGuestServer struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	ateenvv1alpha.UnimplementedFileSystemServiceServer
	receivedChunks atomic.Int32
}

func (g *midStreamUnavailableGuestServer) GetProcess(ctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
	_ = grpc.SetTrailer(ctx, metadata.Pairs("x-ate-env-guest-caps", "start-request-id"))
	if req.GetProcessId() == "ate-env-wake-probe" {
		return nil, status.Error(codes.NotFound, "probe ok")
	}
	return nil, status.Error(codes.Unavailable, "unexpected call")
}

func (g *midStreamUnavailableGuestServer) WriteFile(stream grpc.ClientStreamingServer[ateenvv1alpha.WriteFileRequest, ateenvv1alpha.WriteFileResponse]) error {
	_, err := stream.Recv()
	if err != nil {
		return err
	}
	g.receivedChunks.Add(1)
	return status.Error(codes.Unavailable, "guest crashed mid-stream")
}

// TestReactiveWakeEvictsCacheOnPostRetryUnavailable checks WF-11 and WF-12:
// when a fast-path call gets Unavailable, ensureAwake succeeds and re-adds the
// environment to awakeEnvs, and the post-wake retry also gets Unavailable, the
// second evictOnGuestUnavailable call must remove the environment from
// awakeEnvs.
func TestReactiveWakeEvictsCacheOnPostRetryUnavailable(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	stub := &coldUnavailableGuestServer{}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, stub)
	ateenvv1alpha.RegisterFileSystemServiceServer(grpcSrv, stub)
	t.Cleanup(grpcSrv.Stop)

	cases := []struct {
		name string
		call func(envCtx context.Context) error
	}{
		{
			name: "GetProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.GetProcess(envCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "p-1"})
				return err
			},
		},
		{
			name: "StartProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.StartProcess(envCtx, &ateenvv1alpha.StartProcessRequest{Command: []string{"true"}})
				return err
			},
		},
		{
			name: "ResizeProcess",
			call: func(envCtx context.Context) error {
				_, err := te.procClient.ResizeProcess(envCtx, &ateenvv1alpha.ResizeProcessRequest{ProcessId: "p-1", Rows: 24, Cols: 80})
				return err
			},
		},
		{
			name: "StreamProcessOutput",
			call: func(envCtx context.Context) error {
				stream, err := te.procClient.StreamProcessOutput(envCtx, &ateenvv1alpha.StreamProcessOutputRequest{ProcessId: "p-1"})
				if err != nil {
					return err
				}
				_, err = stream.Recv()
				return err
			},
		},
		{
			name: "ReadFile",
			call: func(envCtx context.Context) error {
				stream, err := te.fsClient.ReadFile(envCtx, &ateenvv1alpha.ReadFileRequest{Path: "a.txt"})
				if err != nil {
					return err
				}
				_, err = stream.Recv()
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			envID := "retry-unavail-" + strings.ToLower(tc.name)
			if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: envID}); err != nil {
				t.Fatalf("CreateEnvironment(%s): %v", envID, err)
			}
			te.router.Register(envID, grpcSrv)
			envCtx := metadata.AppendToOutgoingContext(ctx, "x-env-id", envID, "x-env-atespace", "ate-env")

			// Seed awakeEnvs and x-ate-env-guest-caps via Wake (which runs ensureAwake).
			if err := te.srv.Wake(ctx, "ate-env", envID); err != nil {
				t.Fatalf("Wake(%s): %v", envID, err)
			}
			if !te.srv.IsAwakeCached("ate-env/" + envID) {
				t.Fatalf("expected %s cached awake after Wake", envID)
			}

			stub.calls.Store(0)
			err := tc.call(envCtx)
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("%s = %v, want codes.Unavailable", tc.name, err)
			}
			if got := stub.calls.Load(); got != 2 {
				t.Fatalf("%s forwarded invocations = %d, want 2 (fast-path + 1 post-wake retry)", tc.name, got)
			}
			if te.srv.IsAwakeCached("ate-env/" + envID) {
				t.Fatalf("%s left %s cached awake after post-retry Unavailable", tc.name, envID)
			}
		})
	}
}

type cancelOnFlagContext struct {
	context.Context
	canceled atomic.Bool
}

func (c *cancelOnFlagContext) Err() error {
	if c.canceled.Load() {
		return context.Canceled
	}
	return c.Context.Err()
}

// TestReactiveWakeKeepsCacheOnCanceledContextUnavailable checks WF-11 and
// WF-12: when the caller's context is cancelled as a forwarded RPC answers
// Unavailable, evictOnGuestUnavailable must treat ctx.Err() != nil as caller
// cancellation rather than evicting awakeEnvs or replaying the call.
func TestReactiveWakeKeepsCacheOnCanceledContextUnavailable(t *testing.T) {
	te := newFullTestEnv(t)
	te.srv.RouterPoolSize = 2
	te.srv.ReactiveWake = true
	t.Cleanup(te.srv.Close)

	ctx := context.Background()
	const envID = "cancel-unavail"
	if _, err := te.envClient.CreateEnvironment(ctx, &ateenvv1alpha.CreateEnvironmentRequest{Id: envID}); err != nil {
		t.Fatalf("CreateEnvironment: %v", err)
	}

	callCtx := &cancelOnFlagContext{
		Context: metadata.NewIncomingContext(ctx, metadata.Pairs("x-env-id", envID, "x-env-atespace", "ate-env")),
	}
	var calls atomic.Int32
	stub := &inspectingGuestServer{
		onGet: func(gctx context.Context, req *ateenvv1alpha.GetProcessRequest) (*ateenvv1alpha.Process, error) {
			_ = grpc.SetTrailer(gctx, metadata.Pairs("x-ate-env-guest-caps", "start-request-id"))
			if req.GetProcessId() == "ate-env-wake-probe" {
				return nil, status.Error(codes.NotFound, "probe ok")
			}
			calls.Add(1)
			callCtx.canceled.Store(true)
			return nil, status.Error(codes.Unavailable, "transport closing on caller cancel")
		},
	}
	grpcSrv := grpc.NewServer()
	ateenvv1alpha.RegisterProcessServiceServer(grpcSrv, stub)
	t.Cleanup(grpcSrv.Stop)
	te.router.Register(envID, grpcSrv)

	if err := te.srv.Wake(ctx, "ate-env", envID); err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if !te.srv.IsAwakeCached("ate-env/" + envID) {
		t.Fatal("expected environment cached awake after Wake")
	}

	_, err := te.srv.GetProcess(callCtx, &ateenvv1alpha.GetProcessRequest{ProcessId: "p-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetProcess = %v, want codes.Unavailable", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("forwarded invocations = %d, want 1 (must not retry when caller ctx is cancelled)", got)
	}
	if !te.srv.IsAwakeCached("ate-env/" + envID) {
		t.Fatal("cancelled caller context must not evict awakeEnvs")
	}
}
