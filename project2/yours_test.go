/*
Your test file.  Nothing here is graded.  Write your own tests as you
go.  Delete or change the examples below.

Useful commands:

    go test .                      run every test in this folder
    go test -run TestSmoke .       run only tests whose name has TestSmoke
    go test -v .                   print each test name as it runs
    go test -race .                catch data races (needed for Part 2)

A good habit: when you find a bug, add a test that would have caught
it.  Then you never chase the same bug twice.
*/

package main

import (
	"net"
	"net/rpc"
	"testing"
	"time"
)

/*
A smoke test.  Starts a single KVServer (no backup), writes a key,
reads it back, and checks the value.  Passes once Part 1 is done.
*/
func TestSmokeSingleServer(t *testing.T) {
	addr := "127.0.0.1:21001"

	kv := MakeKVServer(addr, "")
	defer kv.Kill()

	srv := rpc.NewServer()
	if err := srv.Register(kv); err != nil {
		t.Fatalf("register: %v", err)
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	go srv.Accept(lis)

	time.Sleep(100 * time.Millisecond)

	ck := MakeClerk(addr)
	ck.Put("hello", "world")
	if got := ck.Get("hello"); got != "world" {
		t.Fatalf("Get(hello) = %q, want %q", got, "world")
	}
}

func startTestServer(t *testing.T, addr, backup string) *KVServer {
	t.Helper()
	kv := MakeKVServer(addr, backup)
	srv := rpc.NewServer()
	if err := srv.Register(kv); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Accept(l)
	t.Cleanup(func() { kv.Kill(); l.Close() })
	return kv
}

func TestPutThenGet(t *testing.T) {
	addr := "127.0.0.1:21001"

	startTestServer(t, addr, "")
	ck := MakeClerk(addr)

	if got := ck.Get("missing"); got != "" {
		t.Fatalf("missing key: got %q, want empty", got)
	}

	ck.Put("k", "v")

	for i := 0; i < 2; i++ {
		if got := ck.Get("k"); got != "v" {
			t.Fatalf("Get #%d = %q, want v", i+1, got)
		}
	}

	ck.Put("k", "v2") // overwrite
	if got := ck.Get("k"); got != "v2" {
		t.Fatalf("after overwrite got %q, want v2", got)
	}
}

func TestPutRetryAtMostOnce(t *testing.T) {
	addr := "127.0.0.1:19102"

	kv := startTestServer(t, addr, "")

	var r PutAppendReply
	kv.Put(PutAppendArgs{Key: "k", Value: "A", Op: OpPut, Xid: 1001}, &r)
	if r.Err != OK {
		t.Fatalf("first Put: %v", r.Err)
	}
	// another client overwrites
	kv.Put(PutAppendArgs{Key: "k", Value: "B", Op: OpPut, Xid: 2002}, &r)

	// the stale retry of Xid 1001 arrives late: must NOT revert k to "A"
	kv.Put(PutAppendArgs{Key: "k", Value: "A", Op: OpPut, Xid: 1001}, &r)
	if r.Err != OK {
		t.Fatalf("retry should still reply OK, got %v", r.Err)
	}

	var g GetReply
	kv.Get(GetArgs{Key: "k", Xid: 3003}, &g)
	if g.Value != "B" {
		t.Fatalf("retried Put was re-applied: got %q, want B", g.Value)
	}
}

func TestGetRetrySameReply(t *testing.T) {
	addr := "127.0.0.1:19103"

	kv := startTestServer(t, addr, "")
	var pr PutAppendReply
	var g1, g2 GetReply

	kv.Put(PutAppendArgs{Key: "k", Value: "old", Op: OpPut, Xid: 1}, &pr)
	kv.Get(GetArgs{Key: "k", Xid: 50}, &g1)
	kv.Put(PutAppendArgs{Key: "k", Value: "new", Op: OpPut, Xid: 2}, &pr)
	kv.Get(GetArgs{Key: "k", Xid: 50}, &g2) // retry of Xid 50

	if g1.Value != "old" || g2.Value != "old" {
		t.Fatalf("got %q then %q, want old both times", g1.Value, g2.Value)
	}
}

func TestConcurrentClerks(t *testing.T) {
	addr := "127.0.0.1:19301"

	startTestServer(t, addr, "")

	done := make(chan bool, 2)

	go func() {
		ck := MakeClerk(addr)
		for i := 0; i < 100; i++ {
			ck.Put("a", "x")
			if ck.Get("a") != "x" {
				done <- false
				return
			}
		}
		done <- true
	}()

	go func() {
		ck := MakeClerk(addr)
		for i := 0; i < 100; i++ {
			ck.Put("b", "y")
			if ck.Get("b") != "y" {
				done <- false
				return
			}
		}
		done <- true
	}()

	for i := 0; i < 2; i++ {
		select {
		case ok := <-done:
			if !ok {
				t.Fatal("a clerk read the wrong value")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out: a clerk is stuck retrying")
		}
	}
}

/*
Template for Part 2.  Uncomment and fill in once ForwardOp works.
*/

// readMap returns one key from a server's map, under its lock.
func readMap(kv *KVServer, key string) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maps[key]
}

func TestSmokePrimaryBackup(t *testing.T) {
	backupAddr := "127.0.0.1:21011"
	primaryAddr := "127.0.0.1:21012"

	backup := MakeKVServer(backupAddr, "")
	// ... register, listen ...
	bsrv := rpc.NewServer()
	if err := bsrv.Register(backup); err != nil {
		t.Fatal(err)
	}
	bl, berr := net.Listen("tcp", backupAddr)
	if berr != nil {
		t.Fatal(berr)
	}
	go bsrv.Accept(bl)
	defer bl.Close()
	defer backup.Kill()

	primary := MakeKVServer(primaryAddr, backupAddr)
	// ... register, listen ...
	psrv := rpc.NewServer()
	if err := psrv.Register(primary); err != nil {
		t.Fatal(err)
	}
	pl, perr := net.Listen("tcp", primaryAddr)
	if perr != nil {
		t.Fatal(perr)
	}
	go psrv.Accept(pl)
	defer pl.Close()
	defer primary.Kill()

	ck := MakeClerk(primaryAddr)
	// Reach into the backup directly and check its state matches.

	done := make(chan bool, 1)
	go func() {
		ck.Put("k", "v")
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out: Put never returned")
	}

	// Put has returned, so the backup must already hold the value.
	bgot := readMap(backup, "k")
	backup.mu.Lock()
	if bgot != "v" {
		t.Fatalf("backup has %q for k, want v", bgot)
	}

	// The primary should match.
	pgot := readMap(primary, "k")
	if pgot != "v" {
		t.Fatalf("primary has %q for k, want v", pgot)
	}
}

func startPair(t *testing.T, primaryAddr, backupAddr string) (primary, backup *KVServer) {
	t.Helper()
	start := func(addr, fwd string) *KVServer {
		kv := MakeKVServer(addr, fwd)
		srv := rpc.NewServer()
		if err := srv.Register(kv); err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		go srv.Accept(l)
		t.Cleanup(func() { kv.Kill(); l.Close() })
		return kv
	}
	backup = start(backupAddr, "")
	primary = start(primaryAddr, backupAddr)
	return
}

func TestBackupHasWriteBeforeReply(t *testing.T) {
	_, backup := startPair(t, "127.0.0.1:19401", "127.0.0.1:19402")
	ck := MakeClerk("127.0.0.1:19401")

	done := make(chan bool, 1)
	go func() {
		ok := true
		for i := 0; i < 100; i++ {
			v := "v1"
			if i%2 == 1 {
				v = "v2"
			}
			ck.Put("k", v)
			if readMap(backup, "k") != v { // checked immediately after Put returns
				ok = false
				break
			}
		}
		done <- ok
	}()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("backup did not have the value when Put returned")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out")
	}
}

// 2. After a burst of writes from two clerks, backup matches primary.
func TestBackupMatchesPrimary(t *testing.T) {
	primary, backup := startPair(t, "127.0.0.1:19403", "127.0.0.1:19404")

	done := make(chan bool, 2)
	go func() {
		ck := MakeClerk("127.0.0.1:19403")
		for i := 0; i < 200; i++ {
			ck.Put("a", "from-clerk-1")
			ck.Put("b", "from-clerk-1")
		}
		done <- true
	}()
	go func() {
		ck := MakeClerk("127.0.0.1:19403")
		for i := 0; i < 200; i++ {
			ck.Put("a", "from-clerk-2") // same keys: the clerks overwrite each other
			ck.Put("b", "from-clerk-2")
		}
		done <- true
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatal("timed out: a clerk is stuck retrying")
		}
	}

	for _, key := range []string{"a", "b"} {
		p, b := readMap(primary, key), readMap(backup, key)
		if p == "" || p != b {
			t.Errorf("key %s: primary=%q backup=%q", key, p, b)
		}
	}
}
