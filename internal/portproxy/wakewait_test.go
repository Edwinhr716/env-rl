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

package portproxy_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/env/internal/portproxy"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stillWaking is the whole answer, minus Date, for a request whose wake
// outlasts the router's hold, as the proxy gave it before X-Env-Wake-Wait
// existed. A request without the header, and a hold that expires, must both
// produce exactly this.
var stillWaking = answer{
	status: http.StatusServiceUnavailable,
	header: http.Header{
		"Content-Length":         {"60"},
		"Content-Type":           {"text/plain; charset=utf-8"},
		"Retry-After":            {"1"},
		"X-Content-Type-Options": {"nosniff"},
		"X-Env-State":            {"RESUMING"},
	},
	body: "environment ate-env/waking port 8000 is still waking; retry\n",
}

type answer struct {
	status int
	header http.Header
	body   string
}

// send sends one request and returns the whole answer, minus Date.
func send(t *testing.T, req *http.Request) answer {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	h := resp.Header.Clone()
	h.Del("Date")
	return answer{status: resp.StatusCode, header: h, body: string(b)}
}

func (a answer) equal(b answer) bool {
	if a.status != b.status || a.body != b.body || len(a.header) != len(b.header) {
		return false
	}
	for k, v := range a.header {
		if len(b.header[k]) != len(v) {
			return false
		}
		for i := range v {
			if b.header[k][i] != v[i] {
				return false
			}
		}
	}
	return true
}

// newRequest builds a request to port 8000 of environment id in the header
// form, or in the path form when pathForm is set.
func newRequest(t *testing.T, method, base, id string, pathForm bool, body io.Reader) *http.Request {
	t.Helper()
	url := base + "/"
	if pathForm {
		url = base + "/v1alpha/envs/" + id + "/ports/8000/"
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	if !pathForm {
		req.Header.Set(portproxy.EnvIDHeader, id)
		req.Header.Set(portproxy.EnvPortHeader, "8000")
	}
	return req
}

// dispatchProxy serves a proxy for s's router behind Dispatch, so that both
// forms reach it, with a 30 s wake budget and what opts adds.
func dispatchProxy(t *testing.T, s *setup, opts portproxy.Options) *httptest.Server {
	t.Helper()
	opts.ConnectAddr = s.routerAddr
	opts.DefaultAtespace = "ate-env"
	opts.HandshakeTimeout = 5 * time.Second
	opts.WakeBudget = 30 * time.Second
	srv := httptest.NewServer(portproxy.Dispatch(mustNew(t, opts), http.NotFoundHandler()))
	t.Cleanup(srv.Close)
	return srv
}

func fixedState(st ateapipb.ActorState) func(context.Context, string, string) (ateapipb.ActorState, error) {
	return func(context.Context, string, string) (ateapipb.ActorState, error) { return st, nil }
}

// wake is a stand-in for apiservice.(*Server).Wake that counts its calls.
type wake struct {
	calls atomic.Int32
	// fn is the wait; nil blocks until the hold or the caller ends, like a
	// wait on an environment that stays suspended.
	fn func(ctx context.Context, atespace, id string) error
}

func (k *wake) Wake(ctx context.Context, atespace, id string) error {
	k.calls.Add(1)
	if k.fn == nil {
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	return k.fn(ctx, atespace, id)
}

// daemon is a port handler that records the requests it gets.
type daemon struct {
	mu     sync.Mutex
	bodies []string
	header []http.Header
}

func (d *daemon) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	d.mu.Lock()
	d.bodies = append(d.bodies, string(b))
	d.header = append(d.header, r.Header.Clone())
	d.mu.Unlock()
	io.WriteString(w, "served")
}

func (d *daemon) requests() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.bodies)
}

// Without X-Env-Wake-Wait a wake that outlasts the router's hold gets the
// 503 with Retry-After that it got before the header existed, in both forms,
// and the proxy does not call the wake wait. The literal in stillWaking was
// first checked against the proxy without the feature.
func TestNoWakeWaitHeaderAnswersAsBefore(t *testing.T) {
	for _, pathForm := range []bool{false, true} {
		s := newSetup(t)
		s.router.RegisterPort("waking", 8000, &daemon{})
		s.router.Running = func(string) bool { return false }
		s.router.EnvoyTimeout = 200 * time.Millisecond
		k := &wake{}
		srv := dispatchProxy(t, s, portproxy.Options{
			ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			Wake:       k.Wake, WakeWaitCap: time.Minute,
		})
		if got := send(t, newRequest(t, http.MethodGet, srv.URL, "waking", pathForm, nil)); !got.equal(stillWaking) {
			t.Errorf("path form %v: got %+v, want %+v", pathForm, got, stillWaking)
		}
		if n := k.calls.Load(); n != 0 {
			t.Errorf("path form %v: the wake wait ran %d times without the header, want 0", pathForm, n)
		}
	}
}

// With the header the proxy forwards nothing until the wake wait returns,
// then forwards the request once: the daemon gets one request with the whole
// body and without the header, and the caller gets the daemon's answer as it
// streams.
func TestWakeWaitHoldsUntilTheEnvironmentServes(t *testing.T) {
	for _, pathForm := range []bool{false, true} {
		s := newSetup(t)
		var awake atomic.Bool
		s.router.Running = func(string) bool { return awake.Load() }
		d := &daemon{}
		firstRead := make(chan struct{})
		s.router.RegisterPort("waking", 8000, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !awake.Load() {
				t.Error("the daemon got a request before the wake wait returned")
			}
			d.ServeHTTP(w, r)
			w.(http.Flusher).Flush()
			<-firstRead // the caller has the first chunk before the answer ends
			io.WriteString(w, " twice")
		}))
		k := &wake{fn: func(ctx context.Context, atespace, id string) error {
			if atespace != "ate-env" || id != "waking" {
				t.Errorf("wake wait for %s/%s, want ate-env/waking", atespace, id)
			}
			if n := s.router.connectCount(); n != 0 {
				t.Errorf("%d CONNECTs before the wake wait returned, want 0", n)
			}
			time.Sleep(300 * time.Millisecond)
			awake.Store(true)
			return nil
		}}
		// No EnvoyTimeout: a request forwarded before the wake would get the
		// router's 503 at once.
		srv := dispatchProxy(t, s, portproxy.Options{
			ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			Wake:       k.Wake, WakeWaitCap: time.Minute,
		})
		req := newRequest(t, http.MethodPost, srv.URL, "waking", pathForm, strings.NewReader("the whole body"))
		req.Header.Set(portproxy.EnvWakeWaitHeader, "30")
		start := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		chunk := make([]byte, len("served"))
		if _, err := io.ReadFull(resp.Body, chunk); err != nil || string(chunk) != "served" {
			t.Fatalf("path form %v: first chunk %q, %v; want \"served\" before the answer ends", pathForm, chunk, err)
		}
		close(firstRead)
		rest, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(rest) != " twice" {
			t.Errorf("path form %v: got %d and %q after the first chunk, want 200 and \" twice\"", pathForm, resp.StatusCode, rest)
		}
		if took := time.Since(start); took < 300*time.Millisecond {
			t.Errorf("path form %v: answered after %s, before the wake", pathForm, took)
		}
		if n := k.calls.Load(); n != 1 {
			t.Errorf("path form %v: the wake wait ran %d times, want 1", pathForm, n)
		}
		if n := d.requests(); n != 1 {
			t.Fatalf("path form %v: the daemon got %d requests, want 1", pathForm, n)
		}
		if d.bodies[0] != "the whole body" {
			t.Errorf("path form %v: the daemon got the body %q, want it whole and once", pathForm, d.bodies[0])
		}
		if v, ok := d.header[0][portproxy.EnvWakeWaitHeader]; ok {
			t.Errorf("path form %v: the daemon got %s %q, want it consumed", pathForm, portproxy.EnvWakeWaitHeader, v)
		}
		if n := s.router.connectCount(); n != 1 {
			t.Errorf("path form %v: %d CONNECTs, want 1", pathForm, n)
		}
	}
}

// A hold that ends while the environment still wakes gets the 503 with
// Retry-After of a request without the header, and nothing was forwarded.
func TestWakeWaitExpiresWithTheSame503(t *testing.T) {
	s := newSetup(t)
	d := &daemon{}
	s.router.RegisterPort("waking", 8000, d)
	s.router.Running = func(string) bool { return false }
	k := &wake{}
	srv := dispatchProxy(t, s, portproxy.Options{
		ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
		Wake:       k.Wake, WakeWaitCap: time.Minute,
	})
	req := newRequest(t, http.MethodPost, srv.URL, "waking", false, strings.NewReader("body"))
	req.Header.Set(portproxy.EnvWakeWaitHeader, "1")
	start := time.Now()
	got := send(t, req)
	if !got.equal(stillWaking) {
		t.Errorf("got %+v, want %+v", got, stillWaking)
	}
	if took := time.Since(start); took < time.Second || took > 4*time.Second {
		t.Errorf("answered after %s, want the 1 s of the header", took)
	}
	if n := s.router.connectCount(); n != 0 || d.requests() != 0 {
		t.Errorf("%d CONNECTs and %d daemon requests, want nothing forwarded", n, d.requests())
	}
}

// A value above the cap is held for the cap. With a cap of zero no request
// is held, and the header still does not reach the daemon.
func TestWakeWaitCap(t *testing.T) {
	s := newSetup(t)
	d := &daemon{}
	s.router.RegisterPort("waking", 8000, d)
	s.router.Running = func(string) bool { return false }
	var left atomic.Int64
	k := &wake{fn: func(ctx context.Context, _, _ string) error {
		if by, ok := ctx.Deadline(); ok {
			left.Store(int64(time.Until(by)))
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	srv := dispatchProxy(t, s, portproxy.Options{
		ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
		Wake:       k.Wake, WakeWaitCap: 150 * time.Millisecond,
	})
	req := newRequest(t, http.MethodGet, srv.URL, "waking", false, nil)
	req.Header.Set(portproxy.EnvWakeWaitHeader, "5")
	start := time.Now()
	if got := send(t, req); !got.equal(stillWaking) {
		t.Errorf("got %+v, want %+v", got, stillWaking)
	}
	if took := time.Since(start); took < 150*time.Millisecond || took > 2*time.Second {
		t.Errorf("answered after %s, want the 150 ms cap", took)
	}
	if l := time.Duration(left.Load()); l <= 0 || l > 150*time.Millisecond {
		t.Errorf("the wake wait got %s, want at most the 150 ms cap", l)
	}

	s.router.Running = nil // every actor runs
	off := &wake{}
	srv = dispatchProxy(t, s, portproxy.Options{Wake: off.Wake})
	req = newRequest(t, http.MethodGet, srv.URL, "waking", false, nil)
	req.Header.Set(portproxy.EnvWakeWaitHeader, "30")
	if got := send(t, req); got.status != http.StatusOK || got.body != "served" {
		t.Errorf("cap 0: got %d %q, want the daemon's answer", got.status, got.body)
	}
	if n := off.calls.Load(); n != 0 {
		t.Errorf("cap 0: the wake wait ran %d times, want 0", n)
	}
	if v, ok := d.header[len(d.header)-1][portproxy.EnvWakeWaitHeader]; ok {
		t.Errorf("cap 0: the daemon got %s %q, want it consumed", portproxy.EnvWakeWaitHeader, v)
	}
}

// A value that is not whole decimal seconds, or a repeated header, is the
// caller's error: 400 naming the header, no wake wait, nothing forwarded.
// 0 is valid and asks for no hold.
func TestInvalidWakeWaitIs400(t *testing.T) {
	s := newSetup(t)
	d := &daemon{}
	s.router.RegisterPort("up", 8000, d)
	k := &wake{}
	srv := dispatchProxy(t, s, portproxy.Options{Wake: k.Wake, WakeWaitCap: time.Minute})
	for _, values := range [][]string{{""}, {"abc"}, {"1.5"}, {"30s"}, {"-1"}, {"+1"}, {"01"}, {"1e3"}, {"0x10"}, {"1 0"}, {"1234567890"}, {"5", "5"}} {
		for _, pathForm := range []bool{false, true} {
			req := newRequest(t, http.MethodGet, srv.URL, "up", pathForm, nil)
			req.Header[portproxy.EnvWakeWaitHeader] = values
			if got := send(t, req); got.status != http.StatusBadRequest || !strings.Contains(got.body, portproxy.EnvWakeWaitHeader) {
				t.Errorf("%q, path form %v: got %d %q, want 400 naming %s", values, pathForm, got.status, got.body, portproxy.EnvWakeWaitHeader)
			}
		}
	}
	if k.calls.Load() != 0 || d.requests() != 0 {
		t.Errorf("%d wake waits and %d daemon requests for invalid values, want none", k.calls.Load(), d.requests())
	}
	req := newRequest(t, http.MethodGet, srv.URL, "up", false, nil)
	req.Header.Set(portproxy.EnvWakeWaitHeader, "0")
	if got := send(t, req); got.status != http.StatusOK || k.calls.Load() != 0 {
		t.Errorf("0: got %d after %d wake waits, want the daemon's 200 and no wait", got.status, k.calls.Load())
	}
}

// A crashed or deleting environment ends the hold at once, with the answer a
// request without the header gets from the same proxy: 410 or 404 with
// X-Env-State. A deleted one is not held (the router answers the probe) and
// gets the router's 404. In a state that is neither waking nor final the
// request is forwarded.
func TestWakeWaitEndsAtOnceForAnEnvironmentThatWillNotWake(t *testing.T) {
	for _, tc := range []struct {
		name     string
		denyCode int
		denyBody string
		wakeErr  error
		state    ateapipb.ActorState
		stateErr error
		status   int
		header   string
		forwards int // CONNECTs of the held request
	}{
		{"crashed", 503, routerBody("broken", "CRASHED"), status.Error(codes.FailedPrecondition, "crashed while waking"),
			ateapipb.ActorState_ACTOR_STATE_CRASHED, nil, http.StatusGone, "CRASHED", 0},
		{"deleting", 503, routerBody("broken", "DELETING"), status.Error(codes.NotFound, "is being deleted"),
			ateapipb.ActorState_ACTOR_STATE_DELETING, nil, http.StatusNotFound, "DELETING", 0},
		{"deleted during the hold", 503, routerBody("broken", "DELETING"), status.Error(codes.NotFound, "environment not found"),
			0, status.Error(codes.NotFound, "environment not found"), http.StatusNotFound, "", 0},
		{"deleted", 404, "actor ate-env/broken not found", nil,
			0, status.Error(codes.NotFound, "environment not found"), http.StatusNotFound, "", 1},
		{"running behind a failed wait", 0, "", status.Error(codes.Unavailable, "did not answer within the running budget"),
			ateapipb.ActorState_ACTOR_STATE_RUNNING, nil, http.StatusOK, "", 1},
	} {
		s := newSetup(t)
		s.router.RegisterPort("broken", 8000, &daemon{})
		s.router.Deny = func(string) (int, string) { return tc.denyCode, tc.denyBody }
		k := &wake{fn: func(context.Context, string, string) error { return tc.wakeErr }}
		var crashed atomic.Int32
		srv := dispatchProxy(t, s, portproxy.Options{
			ActorState: func(context.Context, string, string) (ateapipb.ActorState, error) { return tc.state, tc.stateErr },
			Wake:       k.Wake, WakeWaitCap: time.Minute,
			OnCrashed: func(string, string) { crashed.Add(1) },
		})
		req := newRequest(t, http.MethodGet, srv.URL, "broken", false, nil)
		req.Header.Set(portproxy.EnvWakeWaitHeader, "30")
		start := time.Now()
		held := send(t, req)
		if took := time.Since(start); took > time.Second {
			t.Errorf("%s: answered after %s, want at once", tc.name, took)
		}
		if held.status != tc.status || held.header.Get(portproxy.EnvStateHeader) != tc.header {
			t.Errorf("%s: got %d %s=%q, want %d %q", tc.name, held.status, portproxy.EnvStateHeader, held.header.Get(portproxy.EnvStateHeader), tc.status, tc.header)
		}
		if n := s.router.connectCount(); n != tc.forwards {
			t.Errorf("%s: %d CONNECTs for the held request, want %d", tc.name, n, tc.forwards)
		}
		if want := int32(0); tc.header == "CRASHED" && crashed.Load() != 1 || tc.header != "CRASHED" && crashed.Load() != want {
			t.Errorf("%s: OnCrashed ran %d times", tc.name, crashed.Load())
		}
		if plain := send(t, newRequest(t, http.MethodGet, srv.URL, "broken", false, nil)); !held.equal(plain) {
			t.Errorf("%s: with the header %+v, without it %+v; want the same answer", tc.name, held, plain)
		}
	}
}

// A caller that leaves ends the hold at once: the wake wait returns, the
// handler returns, and nothing is forwarded. Over HTTP/1.x that holds for a
// request without a body and for one whose body the hold reads ahead (up to
// 64 KiB); behind a longer body the server cannot see the caller leave, and
// the cap ends the hold.
func TestCallerLeavingEndsTheHold(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    int
		cap     time.Duration
		ends    error
		endsBy  time.Duration
		minHold time.Duration
	}{
		{"no body", 0, time.Minute, context.Canceled, 2 * time.Second, 0},
		{"small body", 1 << 10, time.Minute, context.Canceled, 2 * time.Second, 0},
		{"body at the read-ahead limit", 64 << 10, time.Minute, context.Canceled, 2 * time.Second, 0},
		{"body above the read-ahead limit", 64<<10 + 1, time.Second, context.DeadlineExceeded, 4 * time.Second, time.Second},
	} {
		s := newSetup(t)
		s.router.RegisterPort("waking", 8000, &daemon{})
		s.router.Running = func(string) bool { return false }
		waitEnded := make(chan error, 1)
		k := &wake{fn: func(ctx context.Context, _, _ string) error {
			<-ctx.Done()
			waitEnded <- context.Cause(ctx)
			return ctx.Err()
		}}
		p := mustNew(t, portproxy.Options{
			ConnectAddr: s.routerAddr, DefaultAtespace: "ate-env", HandshakeTimeout: 5 * time.Second,
			ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
			Wake:       k.Wake, WakeWaitCap: tc.cap,
		})
		handlerEnded := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerEnded)
			p.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		var body io.Reader
		if tc.body > 0 {
			body = strings.NewReader(strings.Repeat("b", tc.body))
		}
		req := newRequest(t, http.MethodPost, srv.URL, "waking", false, body)
		req.Header.Set(portproxy.EnvWakeWaitHeader, "60")
		start := time.Now()
		if resp, err := (&http.Client{Timeout: 200 * time.Millisecond}).Do(req); err == nil {
			resp.Body.Close()
			t.Fatalf("%s: got %d, want the client's own timeout", tc.name, resp.StatusCode)
		}
		select {
		case err := <-waitEnded:
			if !errors.Is(err, tc.ends) {
				t.Errorf("%s: the wake wait ended with %v, want %v", tc.name, err, tc.ends)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the wake wait still runs 10 s after the caller left", tc.name)
		}
		select {
		case <-handlerEnded:
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: the handler still runs 10 s after the caller left", tc.name)
		}
		if took := time.Since(start); took > tc.endsBy || took < tc.minHold {
			t.Errorf("%s: the hold ended %s after the request started, want between %s and %s", tc.name, took, tc.minHold, tc.endsBy)
		}
		if n := s.router.connectCount(); n != 0 {
			t.Errorf("%s: %d CONNECTs for a caller that left, want 0", tc.name, n)
		}
	}
}

// Over HTTP/2 a caller that leaves resets the stream, which ends the hold at
// once whatever the body.
func TestCallerLeavingEndsTheHoldOverHTTP2(t *testing.T) {
	s := newSetup(t)
	s.router.RegisterPort("waking", 8000, &daemon{})
	s.router.Running = func(string) bool { return false }
	waitEnded := make(chan error, 1)
	k := &wake{fn: func(ctx context.Context, _, _ string) error {
		<-ctx.Done()
		waitEnded <- context.Cause(ctx)
		return ctx.Err()
	}}
	p := mustNew(t, portproxy.Options{
		ConnectAddr: s.routerAddr, DefaultAtespace: "ate-env", HandshakeTimeout: 5 * time.Second,
		ActorState: fixedState(ateapipb.ActorState_ACTOR_STATE_RESUMING),
		Wake:       k.Wake, WakeWaitCap: time.Minute,
	})
	srv := httptest.NewUnstartedServer(p)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req := newRequest(t, http.MethodPost, srv.URL, "waking", false, strings.NewReader(strings.Repeat("b", 1<<20))).WithContext(ctx)
	req.Header.Set(portproxy.EnvWakeWaitHeader, "60")
	start := time.Now()
	if resp, err := srv.Client().Do(req); err == nil {
		resp.Body.Close()
		t.Fatalf("got %d over %s, want the client's own timeout", resp.StatusCode, resp.Proto)
	}
	select {
	case err := <-waitEnded:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the wake wait ended with %v, want the caller's cancel", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the wake wait still runs 10 s after the caller left")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("the hold ended %s after the request started, want soon after the 200 ms timeout", took)
	}
}

// The hold comes after the caller check: a request without a valid
// credential gets 401 and holds nothing.
func TestWakeWaitNeedsTheCredential(t *testing.T) {
	s := newSetup(t)
	s.router.RegisterPort("waking", 8000, &daemon{})
	k := &wake{}
	srv := dispatchProxy(t, s, portproxy.Options{
		Wake: k.Wake, WakeWaitCap: time.Minute,
		Authenticate: func(credential string) error {
			if credential != "Bearer s3cret" {
				return errors.New("wrong token")
			}
			return nil
		},
	})
	for _, pathForm := range []bool{false, true} {
		req := newRequest(t, http.MethodGet, srv.URL, "waking", pathForm, nil)
		req.Header.Set(portproxy.EnvWakeWaitHeader, "30")
		if got := send(t, req); got.status != http.StatusUnauthorized {
			t.Errorf("path form %v: got %d, want 401", pathForm, got.status)
		}
	}
	if n := k.calls.Load(); n != 0 {
		t.Errorf("the wake wait ran %d times for callers without a credential, want 0", n)
	}
}
