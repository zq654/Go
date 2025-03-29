package raft

//
// Raft tests.
//
// we will use the original raft_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"fmt"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/tester1"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const RaftElectionTimeout = 1000 * time.Millisecond

func TestInitialElection3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3A): initial election")

	// is a leader elected?
	ts.checkOneLeader()

	// sleep a bit to avoid racing with followers learning of the
	// election, then check that all peers agree on the term.
	time.Sleep(50 * time.Millisecond)
	term1 := ts.checkTerms()
	if term1 < 1 {
		ts.t.Fatalf("term is %v, but should be at least 1", term1)
	}

	// does the leader+term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := ts.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	// there should still be a leader.
	ts.checkOneLeader()
}

func TestReElection3A(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3A): election after network failure")

	leader1 := ts.checkOneLeader()

	// if the leader disconnects, a new one should be elected.
	ts.g.DisconnectAll(leader1)
	ts.checkOneLeader()

	// if the old leader rejoins, that shouldn't
	// disturb the new leader. and the old leader
	// should switch to follower.
	ts.g.ConnectOne(leader1)
	leader2 := ts.checkOneLeader()

	// if there's no quorum, no new leader should
	// be elected.
	ts.g.DisconnectAll(leader2)
	ts.g.DisconnectAll((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)

	// check that the one connected server
	// does not think it is the leader.
	ts.checkNoLeader()

	// if a quorum arises, it should elect a leader.
	ts.g.ConnectOne((leader2 + 1) % servers)
	ts.checkOneLeader()

	// re-join of last node shouldn't prevent leader from existing.
	ts.g.ConnectOne(leader2)
	ts.checkOneLeader()
}

func TestManyElections3A(t *testing.T) {
	servers := 7
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3A): multiple elections")

	ts.checkOneLeader()

	iters := 10
	for ii := 1; ii < iters; ii++ {
		// disconnect three nodes
		i1 := rand.Int() % servers
		i2 := rand.Int() % servers
		i3 := rand.Int() % servers
		ts.g.DisconnectAll(i1)
		ts.g.DisconnectAll(i2)
		ts.g.DisconnectAll(i3)

		// either the current leader should still be alive,
		// or the remaining four should elect a new one.
		ts.checkOneLeader()

		ts.g.ConnectOne(i1)
		ts.g.ConnectOne(i2)
		ts.g.ConnectOne(i3)
	}
	ts.checkOneLeader()
}

func TestBasicAgree3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): basic agreement")

	iters := 3
	for index := 1; index < iters+1; index++ {
		nd, _ := ts.nCommitted(index)
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}

		xindex := ts.one(index*100, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}
}

// check, based on counting bytes of RPCs, that
// each command is sent to each peer just once.
func TestRPCBytes3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): RPC byte count")

	ts.one(99, servers, false)
	bytes0 := ts.BytesTotal()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters+2; index++ {
		cmd := tester.Randstring(5000)
		xindex := ts.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := ts.BytesTotal()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected+50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

}

// test just failure of followers.
func TestFollowerFailure3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): test progressive failure of followers")

	ts.one(101, servers, false)

	// disconnect one follower from the network.
	leader1 := ts.checkOneLeader()
	ts.g.DisconnectAll((leader1 + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	ts.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.one(103, servers-1, false)

	// disconnect the remaining follower
	leader2 := ts.checkOneLeader()
	ts.g.DisconnectAll((leader2 + 1) % servers)
	ts.g.DisconnectAll((leader2 + 2) % servers)

	// submit a command.
	index, _, ok := ts.srvs[leader2].Raft().Start(104)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := ts.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

}

// test just failure of leaders.
func TestLeaderFailure3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): test failure of leaders")

	ts.one(101, servers, false)

	// disconnect the first leader.
	leader1 := ts.checkOneLeader()
	ts.g.DisconnectAll(leader1)

	// the remaining followers should elect
	// a new leader.
	ts.one(102, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.one(103, servers-1, false)

	// disconnect the new leader.
	leader2 := ts.checkOneLeader()
	ts.g.DisconnectAll(leader2)

	// submit a command to each server.
	for i := 0; i < servers; i++ {
		ts.srvs[i].Raft().Start(104)
	}

	time.Sleep(2 * RaftElectionTimeout)

	// check that command 104 did not commit.
	n, _ := ts.nCommitted(4)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

}

// test that a follower participates after
// disconnect and re-connect.
func TestFailAgree3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): agreement after follower reconnects")

	ts.one(101, servers, false)

	// disconnect one follower from the network.
	leader := ts.checkOneLeader()
	ts.g.DisconnectAll((leader + 1) % servers)

	// the leader and remaining follower should be
	// able to agree despite the disconnected follower.
	ts.one(102, servers-1, false)
	ts.one(103, servers-1, false)
	time.Sleep(RaftElectionTimeout)
	ts.one(104, servers-1, false)
	ts.one(105, servers-1, false)

	// re-connect
	ts.g.ConnectOne((leader + 1) % servers)

	// the full set of servers should preserve
	// previous agreements, and be able to agree
	// on new commands.
	ts.one(106, servers, true)
	time.Sleep(RaftElectionTimeout)
	ts.one(107, servers, true)

}

func TestFailNoAgree3B(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): no agreement if too many followers disconnect")

	ts.one(10, servers, false)

	// 3 of 5 followers disconnect
	leader := ts.checkOneLeader()
	ts.g.DisconnectAll((leader + 1) % servers)
	ts.g.DisconnectAll((leader + 2) % servers)
	ts.g.DisconnectAll((leader + 3) % servers)

	index, _, ok := ts.srvs[leader].Raft().Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}

	time.Sleep(2 * RaftElectionTimeout)

	n, _ := ts.nCommitted(index)
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}

	// repair
	ts.g.ConnectOne((leader + 1) % servers)
	ts.g.ConnectOne((leader + 2) % servers)
	ts.g.ConnectOne((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	leader2 := ts.checkOneLeader()
	index2, _, ok2 := ts.srvs[leader2].Raft().Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	ts.one(1000, servers, true)

}

func TestConcurrentStarts3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): concurrent Start()s")

	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := ts.checkOneLeader()
		_, term, ok := ts.srvs[leader].Raft().Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}

		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := ts.srvs[leader].Raft().Start(100 + i)
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			if t, _ := ts.srvs[j].Raft().GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			cmd := ts.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

}

func TestRejoin3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): rejoin of partitioned leader")

	ts.one(101, servers, true)

	// leader network failure
	leader1 := ts.checkOneLeader()
	ts.g.DisconnectAll(leader1)

	// make old leader try to agree on some entries
	ts.srvs[leader1].Raft().Start(102)
	ts.srvs[leader1].Raft().Start(103)
	ts.srvs[leader1].Raft().Start(104)

	// new leader commits, also for index=2
	ts.one(103, 2, true)

	// new leader network failure
	leader2 := ts.checkOneLeader()
	ts.g.DisconnectAll(leader2)

	// old leader connected again
	ts.g.ConnectOne(leader1)

	ts.one(104, 2, true)

	// all together now
	ts.g.ConnectOne(leader2)

	ts.one(105, servers, true)

}

func TestBackup3B(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): leader backs up quickly over incorrect follower logs")

	ts.one(rand.Int(), servers, true)

	// put leader and one follower in a partition
	leader1 := ts.checkOneLeader()
	ts.g.DisconnectAll((leader1 + 2) % servers)
	ts.g.DisconnectAll((leader1 + 3) % servers)
	ts.g.DisconnectAll((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		ts.srvs[leader1].Raft().Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	ts.g.DisconnectAll((leader1 + 0) % servers)
	ts.g.DisconnectAll((leader1 + 1) % servers)

	// allow other partition to recover
	ts.g.ConnectOne((leader1 + 2) % servers)
	ts.g.ConnectOne((leader1 + 3) % servers)
	ts.g.ConnectOne((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		ts.one(rand.Int(), 3, true)
	}

	// now another partitioned leader and one follower
	leader2 := ts.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	ts.g.DisconnectAll(other)

	// lots more commands that won't commit
	for i := 0; i < 50; i++ {
		ts.srvs[leader2].Raft().Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		ts.g.DisconnectAll(i)
	}
	ts.g.ConnectOne((leader1 + 0) % servers)
	ts.g.ConnectOne((leader1 + 1) % servers)
	ts.g.ConnectOne(other)

	// lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		ts.one(rand.Int(), 3, true)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		ts.g.ConnectOne(i)
	}
	ts.one(rand.Int(), servers, true)
}

func TestCount3B(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3B): RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += ts.g.RpcCount(j)
		}
		return
	}

	leader := ts.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = ts.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := ts.srvs[leader].Raft().Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := ts.srvs[leader].Raft().Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := ts.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := ts.srvs[j].Raft().GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += ts.g.RpcCount(j)
		}

		if failed {
			continue loop
		}

		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += ts.g.RpcCount(j)
	}

	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

}

func TestPersist13C(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): basic persistence")

	ts.one(11, servers, true)

	ts.g.Shutdown()
	ts.g.StartServers()

	ts.one(12, servers, true)

	leader1 := ts.checkOneLeader()
	ts.g.ShutdownServer(leader1)
	ts.restart(leader1)

	ts.one(13, servers, true)

	leader2 := ts.checkOneLeader()
	ts.g.ShutdownServer(leader2)

	ts.one(14, servers-1, true)

	ts.restart(leader2)

	ts.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (ts.checkOneLeader() + 1) % servers
	ts.g.ShutdownServer(i3)

	ts.one(15, servers-1, true)

	ts.restart(i3)

	ts.one(16, servers, true)
}

func TestPersist23C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		ts.one(10+index, servers, true)
		index++

		leader1 := ts.checkOneLeader()

		ts.g.ShutdownServer((leader1 + 1) % servers)
		ts.g.ShutdownServer((leader1 + 2) % servers)

		ts.one(10+index, servers-2, true)
		index++

		ts.g.ShutdownServer((leader1 + 0) % servers)
		ts.g.ShutdownServer((leader1 + 3) % servers)
		ts.g.ShutdownServer((leader1 + 4) % servers)

		ts.restart((leader1 + 1) % servers)
		ts.restart((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		ts.restart((leader1 + 3) % servers)

		ts.one(10+index, servers-2, true)
		index++

		ts.restart((leader1 + 4) % servers)
		ts.restart((leader1 + 0) % servers)
	}

	ts.one(1000, servers, true)
}

func TestPersist33C(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): partitioned leader and one follower crash, leader restarts")

	ts.one(101, 3, true)

	leader := ts.checkOneLeader()
	ts.g.DisconnectAll((leader + 2) % servers)

	ts.one(102, 2, true)

	ts.g.ShutdownServer((leader + 0) % servers)
	ts.g.ShutdownServer((leader + 1) % servers)
	ts.restart((leader + 2) % servers)
	ts.restart((leader + 0) % servers)

	ts.one(103, 2, true)

	ts.restart((leader + 1) % servers)

	ts.one(104, servers, true)
}

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
func TestFigure83C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, true, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): Figure 8")

	ts.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if ts.srvs[i].Raft() != nil {
				_, _, ok := ts.srvs[i].Raft().Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			ts.g.ShutdownServer(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if ts.srvs[s].Raft() == nil {
				ts.restart(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if ts.srvs[i].Raft() == nil {
			ts.restart(i)
		}
	}

	ts.one(rand.Int(), servers, true)

}

func TestUnreliableAgree3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				ts.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		ts.one(iters, 1, true)
	}

	ts.SetReliable(true)

	wg.Wait()

	ts.one(100, servers, true)

}

func TestFigure8Unreliable3C(t *testing.T) {
	servers := 5
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	ts.Begin("Test (3C): Figure 8 (unreliable)")

	ts.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			ts.SetLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := ts.srvs[i].Raft().Start(rand.Int() % 10000)
			if ok && ts.g.IsConnected(i) {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			ts.g.DisconnectAll(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if !ts.g.IsConnected(s) {
				ts.g.ConnectOne(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if !ts.g.IsConnected(i) {
			ts.g.ConnectOne(i)
		}
	}

	ts.one(rand.Int()%10000, servers, true)

}

func internalChurn(t *testing.T, reliable bool) {

	servers := 5
	ts := makeTest(t, servers, reliable, false)
	defer ts.cleanup()

	if ts.IsReliable() {
		ts.Begin("Test (3C): churn")
	} else {
		ts.Begin("Test (3C): unreliable churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				ts.mu.Lock()
				rf := ts.srvs[i].Raft()
				ts.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := ts.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							ts.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			ts.g.DisconnectAll(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if ts.srvs[i].raft == nil {
				ts.restart(i)
			}
			ts.g.ConnectOne(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if ts.srvs[i].raft != nil {
				ts.g.ShutdownServer(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	ts.SetReliable(true)
	for i := 0; i < servers; i++ {
		if ts.srvs[i].raft == nil {
			ts.restart(i)
		}
		ts.g.ConnectOne(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := ts.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := ts.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			ts.t.Fatalf("didn't find a value")
		}
	}

}

func TestReliableChurn3C(t *testing.T) {
	internalChurn(t, true)
}

func TestUnreliableChurn3C(t *testing.T) {
	internalChurn(t, false)
}

const (
	MAXLOGSIZE = 2000
)

func snapcommon(t *testing.T, name string, disconnect bool, reliable bool, crash bool) {
	iters := 30
	servers := 3
	ts := makeTest(t, servers, reliable, true)
	defer ts.cleanup()

	ts.Begin(name)

	ts.one(rand.Int(), servers, true)
	leader1 := ts.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if disconnect {
			ts.g.DisconnectAll(victim)
			ts.one(rand.Int(), servers-1, true)
		}
		if crash {
			ts.g.ShutdownServer(victim)
			ts.one(rand.Int(), servers-1, true)
		}

		// perhaps send enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			ts.srvs[sender].Raft().Start(rand.Int())
		}

		// let applier threads catch up with the Start()'s
		if disconnect == false && crash == false {
			// make sure all followers have caught up, so that
			// an InstallSnapshot RPC isn't required for
			// TestSnapshotBasic3D().
			ts.one(rand.Int(), servers, true)
		} else {
			ts.one(rand.Int(), servers-1, true)
		}

		if ts.g.LogSize() >= MAXLOGSIZE {
			ts.t.Fatalf("Log size too large")
		}
		if disconnect {
			// reconnect a follower, who maybe behind and
			// needs to rceive a snapshot to catch up.
			ts.g.ConnectOne(victim)
			ts.one(rand.Int(), servers, true)
			leader1 = ts.checkOneLeader()
		}
		if crash {
			ts.restart(victim)
			ts.one(rand.Int(), servers, true)
			leader1 = ts.checkOneLeader()
		}
	}
}

func TestSnapshotBasic3D(t *testing.T) {
	snapcommon(t, "Test (3D): snapshots basic", false, true, false)
}

func TestSnapshotInstall3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliable3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (disconnect)",
		true, false, false)
}

func TestSnapshotInstallCrash3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrash3D(t *testing.T) {
	snapcommon(t, "Test (3D): install snapshots (crash)", false, false, true)
}

// do the servers persist the snapshots, and
// restart using snapshot along with the
// tail of the log?
func TestSnapshotAllCrash3D(t *testing.T) {
	servers := 3
	iters := 5
	ts := makeTest(t, servers, false, true)
	defer ts.cleanup()

	ts.Begin("Test (3D): crash and restart all servers")

	ts.one(rand.Int(), servers, true)

	for i := 0; i < iters; i++ {
		// perhaps enough to get a snapshot
		nn := (SnapShotInterval / 2) + (rand.Int() % SnapShotInterval)
		for i := 0; i < nn; i++ {
			ts.one(rand.Int(), servers, true)
		}

		index1 := ts.one(rand.Int(), servers, true)

		// crash all
		ts.g.Shutdown()
		ts.g.StartServers()

		index2 := ts.one(rand.Int(), servers, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %v to %v", index1, index2)
		}
	}
}

// do servers correctly initialize their in-memory copy of the snapshot, making
// sure that future writes to persistent state don't lose state?
func TestSnapshotInit3D(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, true)
	defer ts.cleanup()

	ts.Begin("Test (3D): snapshot initialization after crash")
	ts.one(rand.Int(), servers, true)

	// enough ops to make a snapshot
	nn := SnapShotInterval + 1
	for i := 0; i < nn; i++ {
		ts.one(rand.Int(), servers, true)
	}

	ts.g.Shutdown()
	ts.g.StartServers()

	// a single op, to get something to be written back to persistent storage.
	ts.one(rand.Int(), servers, true)

	ts.g.Shutdown()
	ts.g.StartServers()

	// do another op to trigger potential bug
	ts.one(rand.Int(), servers, true)
}
