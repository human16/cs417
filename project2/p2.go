/*
p2.go -- CS 417/545 Project 2.  Primary-Backup Key-Value Store.

This is the ONLY file you edit (plus yours_test.go for your own testing).
Add any helper types, fields, or package-level variables you need.
Do NOT change the signatures or receiver types of any function that
has an EXPORTED comment above it: those are the public API that our
tests call.
*/

package main

import (
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

/* ========================================================================
   Part 1.  Single-Server Key-Value Service
   ======================================================================== */

/*
KVServer is a single RPC server that stores key -> value.  Part 2
extends it with primary-backup replication by adding a backup
address.
*/
type KVServer struct {
	mu     sync.Mutex
	me     string /* this server's RPC address, e.g. "localhost:9001" */
	backup string /* backup's RPC address, "" if this server is the backup */
	dead   int32  /* set to 1 by Kill */

	/*
	   ADD YOUR CODE HERE: your key-value store, your dedup table,
	   and any helper state you need.
	*/

	maps map[string]int // takes in the string for the specific weight and maps to its value
}

/*
EXPORTED
MakeKVServer constructs a server.  If backup is "", this server IS
the backup (or a standalone single-server) and will not forward
anything.  If backup is a non-empty address, this server IS the
primary and must forward every mutating op to that address.  The
caller is responsible for calling net/rpc.Register and starting the
listener.
*/
func MakeKVServer(me, backup string) *KVServer {
	/*
	   ADD YOUR CODE HERE.  Typical shape:
	     kv := &KVServer{me: me, backup: backup}
	     ... initialise maps ...
	     return kv
	*/
	_, _ = me, backup

	kv := &KVServer{me: me, backup: backup}
	kv.dead = 0 // not
	kv.maps = map[string]int{}

	return kv
}

/*
EXPORTED
Get returns the current value for args.Key.  Missing keys return "".
Must be at-most-once: a retried call with the same Xid returns the
same reply as the first call.
*/
func (kv *KVServer) Get(args GetArgs, reply *GetReply) error {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/*
EXPORTED
Put overwrites the value for args.Key.
Must be at-most-once (see Get).
On the primary: must also forward to the backup and wait for its
ack before replying.
*/
func (kv *KVServer) Put(args PutAppendArgs, reply *PutAppendReply) error {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/* ========================================================================
   Part 2.  Primary-Backup Replication
   ======================================================================== */

/*
EXPORTED
ForwardOp is called by the primary on the backup for every mutating
client operation.  The backup applies the operation (respecting
at-most-once) and returns OK.
*/
func (kv *KVServer) ForwardOp(args ForwardArgs, reply *ForwardReply) error {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/* ========================================================================
   Clerk (client-side helper)
   ======================================================================== */

/*
Clerk is the client-side helper type.  It hides retries from the
application.  In P2 the clerk talks to a fixed primary address;
automatic re-routing on failover is scope for the next project.
*/
type Clerk struct {
	mu      sync.Mutex
	primary string /* primary server address */
	xid     uint64 /* atomic counter for fresh Xids */

	/*
	   ADD YOUR CODE HERE: cached RPC client, etc.
	*/
}

/*
EXPORTED
MakeClerk returns a new Clerk that will send operations to the given
primary address.
*/
func MakeClerk(primary string) *Clerk {
	ck := &Clerk{primary: primary}
	/* ADD YOUR CODE HERE */
	return ck
}

/*
nextXid returns a fresh at-most-once id.  Safe for concurrent use.
Every operation MUST carry a fresh Xid, and a retried operation MUST
reuse it.
*/
func (ck *Clerk) nextXid() Xid {
	return Xid(atomic.AddUint64(&ck.xid, 1))
}

/*
EXPORTED
Get returns the current value for key, or "" if the key is missing.
Keeps retrying through RPC timeouts until it gets a reply.
*/
func (ck *Clerk) Get(key string) string {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/*
EXPORTED
Put overwrites the value for key.  Keeps retrying (see Get).
*/
func (ck *Clerk) Put(key, value string) {
	/* ADD YOUR CODE HERE */
	panic("not implemented yet")
}

/* ========================================================================
   Lifecycle helpers.
   ======================================================================== */

/*
EXPORTED
Kill marks the KV server dead.  Running goroutines should notice via
kv.isDead() and exit; subsequent RPCs should return ErrNetwork.
*/
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) isDead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

/* ========================================================================
   Command-line driver.  Section 5 of the writeup documents the flags.
   ======================================================================== */

func main() {
	mode := flag.String("mode", "", "kvserver | clerk")
	addr := flag.String("addr", "", "kvserver: this server's RPC address, e.g. localhost:9001")
	forward := flag.String("forward", "", "kvserver: backup's RPC address to forward writes to; omit to run as the backup / standalone")
	primary := flag.String("primary", "", "clerk: primary server's RPC address")
	op := flag.String("op", "", "clerk: get | put")
	key := flag.String("key", "", "clerk: key")
	val := flag.String("val", "", "clerk: value for put")
	flag.Parse()

	switch *mode {
	case "kvserver":
		if *addr == "" {
			die("kvserver mode needs -addr")
		}
		kv := MakeKVServer(*addr, *forward)
		rpc.Register(kv)
		serveForever(*addr)
	case "clerk":
		if *primary == "" || *op == "" || *key == "" {
			die("clerk mode needs -primary -op -key")
		}
		ck := MakeClerk(*primary)
		switch *op {
		case "get":
			fmt.Println(ck.Get(*key))
		case "put":
			ck.Put(*key, *val)
		default:
			die("unknown -op: " + *op)
		}
	default:
		die("set -mode to kvserver or clerk")
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(2)
}

func serveForever(addr string) {
	/*
	   ADD YOUR CODE HERE: net.Listen on tcp addr, loop over rpc.Accept.
	*/
	panic("not implemented yet")
}
