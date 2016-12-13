package epaxos

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/relab/epaxos/epaxosprotobuf"
	"github.com/relab/epaxos/genericsmr"
	"github.com/relab/epaxos/genericsmrprotobuf"
	"github.com/relab/epaxos/state"
)

// Global handler lock
var ghl sync.Mutex

const MAX_DEPTH_DEP = 10

const ADAPT_TIME_SEC = 10

const MAX_BATCH = 1000

const COMMIT_GRACE_PERIOD = 10 * 1e9 //10 seconds

const DO_CHECKPOINTING = false
const HT_INIT_SIZE = 200000
const CHECKPOINT_PERIOD = 10000

var cpMarker []*epaxosprotobuf.Command
var cpcounter = 0

type Replica struct {
	*genericsmr.Replica

	mgr *epaxosprotobuf.Manager

	InstanceSpace      [][]*Instance // the space of all instances (used and not yet used)
	crtInstance        []int32       // highest active instance numbers that this replica knows about
	CommittedUpTo      []int32       // highest committed instance per replica that this replica knows about
	ExecedUpTo         []int32       // instance up to which all commands have been executed (including iteslf)
	exec               *Exec
	conflicts          []map[int64]int32
	maxSeqPerKey       map[int64]int32
	maxSeq             int32
	latestCPReplica    int32
	latestCPInstance   int32
	clientMutex        *sync.Mutex // for synchronizing when sending replies to clients from multiple go-routines
	instancesToRecover chan *instanceId

	conf *epaxosprotobuf.Configuration
}

type Instance struct {
	Cmds           []*epaxosprotobuf.Command
	ballot         int32
	Status         epaxosprotobuf.OK
	Seq            int32
	Deps           []int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
}

type instanceId struct {
	replica  int32
	instance int32
}

type RecoveryInstance struct {
	cmds            []state.Command
	status          int8
	seq             int32
	deps            []int32
	preAcceptCount  int
	leaderResponded bool
}

type LeaderBookkeeping struct {
	clientProposals   []*genericsmr.Propose
	maxRecvBallot     int32
	prepareOKs        int
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      []int32
	committedDeps     []int32
	recoveryInst      *RecoveryInstance
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaOKs            int
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, beacon bool, durable bool) *Replica {
	r := &Replica{
		genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		nil,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[int64]int32, len(peerAddrList)),
		make(map[int64]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, genericsmr.CHAN_BUFFER_SIZE),
		nil,
	}

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, 16*1024*1024)
		r.crtInstance[i] = 0
		r.ExecedUpTo[i] = -1
		r.CommittedUpTo[i] = -1
		r.conflicts[i] = make(map[int64]int32, HT_INIT_SIZE)
	}

	r.exec = &Exec{r}

	cpMarker = make([]*epaxosprotobuf.Command, 0)

	go r.run()

	return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	b := make([]byte, 9+r.N*4)
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.Status)
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
	l := 9
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []*epaxosprotobuf.Command) {
	if !r.Durable {
		return
	}
	/*
		if cmds == nil {
			return
		}
		for i := 0; i < len(cmds); i++ {
			cmds[i].Marshal(io.Writer(r.StableStore))
		}
	*/
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* Clock goroutine */

var fastClockChan chan bool
var slowClockChan chan bool

func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(5 * 1e6) // 5 ms
		fastClockChan <- true
	}
}
func (r *Replica) slowClock() {
	for !r.Shutdown {
		time.Sleep(150 * 1e6) // 150 ms
		slowClockChan <- true
	}
}

func (r *Replica) stopAdapting() {
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	log.Println(r.PreferredPeerOrder)
}

var conflicted, weird, slow, happy int

/* ============= */

/***********************************
   Main event processing loop      *
************************************/

func init() {
	grpc.EnableTracing = false
}

func (r *Replica) run() {
	grpcListener, err := net.Listen("tcp", r.PeerAddrList[r.Id])
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()
	epaxosprotobuf.RegisterAcceptServiceServer(grpcServer, r)
	epaxosprotobuf.RegisterCommitServiceServer(grpcServer, r)

	go func() {
		err := grpcServer.Serve(grpcListener)
		log.Println("grpc server:", err)
	}()

	log.Println("calling new manager")
	for i := 0; i < 10; i++ {
		r.mgr, err = epaxosprotobuf.NewManager(
			r.PeerAddrList,
			epaxosprotobuf.WithSelfAddr(r.PeerAddrList[r.Id]),
			epaxosprotobuf.WithGrpcDialOptions(
				grpc.WithBlock(),
				grpc.WithInsecure(),
				grpc.WithTimeout(5*time.Second),
			),
		)
		if err != nil {
			log.Println("new manager error:", err)
		}
	}
	if err != nil {
		log.Fatalln("manager could not connect:", err)
	}

	log.Println("manager connected")
	log.Println("replia id", r.Id)
	log.Println("waiting for client connections")

	var ids []uint32
	nodes := r.mgr.Nodes(true)
	for _, node := range nodes {
		ids = append(ids, node.ID())
	}

	q := len(r.PeerAddrList)/2 + 1 // valid for n=3 and n=5, thrifty not implemented
	qspec := &EPaxosQSpec{q: q - 1}

	r.conf, err = r.mgr.NewConfiguration(ids, qspec, time.Second)
	if err != nil {
		panic(err)
	}

	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
	}

	if r.Id == 0 {
		//init quorum read lease
		quorum := make([]int32, r.N/2+1)
		for i := 0; i <= r.N/2; i++ {
			quorum[i] = int32(i)
		}
		r.UpdatePreferredPeerOrder(quorum)
	}

	slowClockChan = make(chan bool, 1)
	fastClockChan = make(chan bool, 1)
	go r.slowClock()

	//Enabled when batching for 5ms
	if MAX_BATCH > 100 {
		go r.fastClock()
	}

	if r.Beacon {
		go r.stopAdapting()
	}

	onOffProposeChan := r.ProposeChan

	for !r.Shutdown {

		select {

		case propose := <-onOffProposeChan:
			// Note: Spawned as goroutine to enable pipelining (as in origianl).
			go r.handlePropose(propose)
			//deactivate new proposals channel to prioritize the handling of other protocol messages,
			//and to allow commands to accumulate for batching
			onOffProposeChan = nil
			break

		case <-fastClockChan:
			//activate new proposals channel
			onOffProposeChan = r.ProposeChan
			break

		case beacon := <-r.BeaconChan:
			log.Printf("received beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
			r.ReplyBeacon(beacon)
			break

		case <-slowClockChan:
			if r.Beacon {
				for q := int32(0); q < int32(r.N); q++ {
					if q == r.Id {
						continue
					}
					r.SendBeacon(q)
				}
			}
			break

		case <-r.OnClientConnect:
			log.Printf("weird %d; conflicted %s; slow %d; happy %d\n", weird, "n/a", slow, happy)
			weird, conflicted, slow, happy = 0, 0, 0, 0

		}
	}
}

/***********************************
   Command execution thread        *
************************************/

func (r *Replica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	problemInstance := make([]int32, r.N)
	timeout := make([]uint64, r.N)
	for q := 0; q < r.N; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}

	for !r.Shutdown {
		executed := false
		for q := 0; q < r.N; q++ {
			inst := int32(0)
			for inst = r.ExecedUpTo[q] + 1; inst < r.crtInstance[q]; inst++ {
				if r.InstanceSpace[q][inst] != nil && r.InstanceSpace[q][inst].Status == epaxosprotobuf.EXECUTED {
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
					continue
				}
				if r.InstanceSpace[q][inst] == nil || r.InstanceSpace[q][inst].Status != epaxosprotobuf.COMMITTED {
					if inst == problemInstance[q] {
						timeout[q] += SLEEP_TIME_NS
						if timeout[q] >= COMMIT_GRACE_PERIOD {
							r.instancesToRecover <- &instanceId{int32(q), inst}
							timeout[q] = 0
						}
					} else {
						problemInstance[q] = inst
						timeout[q] = 0
					}
					if r.InstanceSpace[q][inst] == nil {
						continue
					}
					break
				}
				if ok := r.exec.executeCommand(int32(q), inst); ok {
					executed = true
					if inst == r.ExecedUpTo[q]+1 {
						r.ExecedUpTo[q] = inst
					}
				}
			}
		}
		if !executed {
			time.Sleep(SLEEP_TIME_NS)
		}
		//log.Println(r.ExecedUpTo, " ", r.crtInstance)
	}
}

/* Ballot helper functions */

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
	return r.makeUniqueBallot((ballot >> 4) + 1)
}

func isInitialBallot(ballot int32) bool {
	return (ballot >> 4) == 0
}

func replicaIdFromBallot(ballot int32) int32 {
	return ballot & 15
}

/**********************************************************************
                    inter-replica communication
***********************************************************************/

func (r *Replica) bcastPreAccept(replica int32, instance int32, ballot int32, cmds []*epaxosprotobuf.Command, seq int32, deps []int32) *epaxosprotobuf.PreAccept {
	var pa epaxosprotobuf.PreAccept
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = cmds
	pa.Seq = seq
	pa.Deps = deps
	return &pa
}

func (r *Replica) bcastAccept(replica int32, instance int32, ballot int32, count int32, seq int32, deps []int32) *epaxosprotobuf.Accept {
	var ea epaxosprotobuf.Accept
	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = ballot
	ea.Count = count
	ea.Seq = seq
	ea.Deps = deps
	return &ea
}

func (r *Replica) bcastCommit(replica int32, instance int32, cmds []*epaxosprotobuf.Command, seq int32, deps []int32) *epaxosprotobuf.CommitShort {
	var ecs epaxosprotobuf.CommitShort
	ecs.LeaderId = r.Id
	ecs.Replica = replica
	ecs.Instance = instance
	ecs.Count = int32(len(cmds))
	ecs.Seq = seq
	ecs.Deps = deps
	return &ecs
}

/******************************************************************
               Helper functions
*******************************************************************/

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[int64]int32, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	//log.Println("updatecommitted: entering")
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == epaxosprotobuf.COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == epaxosprotobuf.EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
		//log.Println("updatecommitted: incrementing for replica", replica)
	}
}

func (r *Replica) updateConflicts(cmds []*epaxosprotobuf.Command, replica int32, instance int32, seq int32) {
	for i := 0; i < len(cmds); i++ {
		if d, present := r.conflicts[replica][cmds[i].K]; present {
			if d < instance {
				r.conflicts[replica][cmds[i].K] = instance
			}
		} else {
			r.conflicts[replica][cmds[i].K] = instance
		}
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if s < seq {
				r.maxSeqPerKey[cmds[i].K] = seq
			}
		} else {
			r.maxSeqPerKey[cmds[i].K] = seq
		}
	}
}

func (r *Replica) updateAttributes(cmds []*epaxosprotobuf.Command, seq int32, deps []int32, replica int32, instance int32) (int32, []int32, bool) {
	changed := false
	for q := 0; q < r.N; q++ {
		if r.Id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if d, present := (r.conflicts[q])[cmds[i].K]; present {
				if d > deps[q] {
					deps[q] = d
					if seq <= r.InstanceSpace[q][d].Seq {
						seq = r.InstanceSpace[q][d].Seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for i := 0; i < len(cmds); i++ {
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (r *Replica) mergeAttributes(seq1 int32, deps1 []int32, seq2 int32, deps2 []int32) (int32, []int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < r.N; q++ {
		if int32(q) == r.Id {
			continue
		}
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return seq1, deps1, equal
}

func equal(deps1 []int32, deps2 []int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

/**********************************************************************

                            PHASE 1

***********************************************************************/

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	ghl.Lock()

	batchSize := len(r.ProposeChan) + 1
	if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}

	instNo := r.crtInstance[r.Id]
	r.crtInstance[r.Id]++

	//log.Printf("starting instance %d\n", instNo)
	//log.Printf("batching %d\n", batchSize)

	cmds := make([]*epaxosprotobuf.Command, batchSize)
	proposals := make([]*genericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}

	r.startPhase1(r.Id, instNo, 0, proposals, cmds, batchSize)
}

func (r *Replica) startPhase1(replica int32, instance int32, ballot int32, proposals []*genericsmr.Propose, cmds []*epaxosprotobuf.Command, batchSize int) {
	// init command attributes
	seq := int32(0)
	deps := make([]int32, r.N)
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}

	seq, deps, _ = r.updateAttributes(cmds, seq, deps, replica, instance)

	inst := &Instance{
		cmds,
		ballot,
		epaxosprotobuf.PREACCEPTED,
		seq,
		deps,
		&LeaderBookkeeping{proposals, 0, 0, true, 0, 0, 0, deps, []int32{-1, -1, -1, -1, -1}, nil, false, false, nil, 0}, 0, 0,
	}
	r.InstanceSpace[r.Id][instance] = inst

	r.updateConflicts(cmds, r.Id, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq + 1
	}

	r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	r.recordCommands(cmds)
	r.sync()

	cpcounter += batchSize

	pa := r.bcastPreAccept(r.Id, instance, ballot, cmds, seq, deps)

	ghl.Unlock()

	par, err := r.conf.PreAcceptRPC(pa)
	if err != nil {
		panic(err)
	}
	pareply := par.Reply

	ghl.Lock()

	if inst.Status != epaxosprotobuf.PREACCEPTED {
		// we've moved on, this is a delayed reply
		panic("leader: inst.Status != PREACEPTED")
	}

	if inst.ballot != pareply.Ballot {
		panic("startPhase1: wrong ballot")
	}

	status := pareply.Ok
	if status == epaxosprotobuf.NONE {
		// TODO: there is probably another active leader
		panic("startPhase1: received NACK")
	}

	allCommitted := true
	for q := 0; q < r.N; q++ {
		if inst.lb.committedDeps[q] < inst.lb.originalDeps[q] {
			inst.lb.committedDeps[q] = inst.lb.originalDeps[q]
		}
		if inst.lb.committedDeps[q] < r.CommittedUpTo[q] {
			inst.lb.committedDeps[q] = r.CommittedUpTo[q]
		}
		if inst.lb.committedDeps[q] < inst.Deps[q] {
			allCommitted = false
		}
	}

	if !allCommitted || !isInitialBallot(inst.ballot) || status == epaxosprotobuf.PREACCEPTED {
		r.handlePreAcceptNotEq(pareply, inst, allCommitted)
	} else if status == epaxosprotobuf.PREACCEPTED_EQ {
		r.handlePreAcceptEq(pareply, instance, inst)
	} else {
		panic("startPhase1: invalid pareply.Ok value")
	}
}

func (r *Replica) handlePreAcceptEq(pareply *epaxosprotobuf.PreAcceptReply, instNo int32, inst *Instance) {
	happy++
	r.InstanceSpace[r.Id][instNo].Status = epaxosprotobuf.COMMITTED
	r.updateCommitted(r.Id)
	if inst.lb.clientProposals != nil && !r.Dreply {
		// give clients the all clear
		for i := 0; i < len(inst.lb.clientProposals); i++ {
			r.ReplyProposeTS(
				&genericsmrprotobuf.ProposeReplyTS{
					genericsmrprotobuf.OK,
					inst.lb.clientProposals[i].CommandId,
					0,
					inst.lb.clientProposals[i].Timestamp},
				inst.lb.clientProposals[i].Reply)
		}
	}
	creq := r.bcastCommit(r.Id, instNo, inst.Cmds, inst.Seq, inst.Deps)
	ghl.Unlock()
	err := r.conf.CommitShortRPC(creq)
	if err != nil {
		panic("handlePreAcceptEq: CommitShortRPC error")
	}
}

func (r *Replica) handlePreAcceptNotEq(pareply *epaxosprotobuf.PreAcceptReply, inst *Instance, allCommitted bool) {
	if !allCommitted {
		weird++
	}
	slow++

	inst.Status = epaxosprotobuf.ACCEPTED
	acc := r.bcastAccept(pareply.Replica, pareply.Instance, inst.ballot, int32(len(inst.Cmds)), inst.Seq, inst.Deps)

	ghl.Unlock()

	ar, err := r.conf.AcceptRPC(acc)
	if err != nil {
		panic(err)
	}
	areply := ar.Reply

	ghl.Lock()

	if inst.ballot != areply.Ballot {
		panic("handlePreAccepetNotEq: accept reply wrong ballot")
	}

	if areply.Ok == epaxosprotobuf.NONE {
		// TODO: there is probably another active leader
		panic("handlePreAccepetNotEq: accept reply NACK")
	}

	//r.InstanceSpace[areply.Replica][areply.Instance].Status = epaxosprotobuf.COMMITTED
	//log.Println("handlePreAcceptNotEq: setting committed:", r.Id, areply.Instance)
	r.updateCommitted(areply.Replica)
	if inst.lb.clientProposals != nil && !r.Dreply {
		// give clients the all clear
		for i := 0; i < len(inst.lb.clientProposals); i++ {
			r.ReplyProposeTS(
				&genericsmrprotobuf.ProposeReplyTS{
					genericsmrprotobuf.OK,
					inst.lb.clientProposals[i].CommandId,
					0,
					inst.lb.clientProposals[i].Timestamp},
				inst.lb.clientProposals[i].Reply)
		}
	}

	creq := r.bcastCommit(areply.Replica, areply.Instance, inst.Cmds, inst.Seq, inst.Deps)
	ghl.Unlock()
	err = r.conf.CommitShortRPC(creq)
	if err != nil {
		panic("handlePreAcceptNotEq: CommitShortRPC error")
	}
}

func (r *Replica) PreAcceptRPC(ctx context.Context, pa *epaxosprotobuf.PreAccept) (*epaxosprotobuf.PreAcceptReply, error) {
	reply := r.handlePreAccept(pa)
	if reply == nil {
		return &epaxosprotobuf.PreAcceptReply{}, grpc.Errorf(codes.AlreadyExists, "%s", "instance already committed")
	}
	return reply, nil
}

func (r *Replica) handlePreAccept(preAccept *epaxosprotobuf.PreAccept) *epaxosprotobuf.PreAcceptReply {
	ghl.Lock()
	defer ghl.Unlock()

	inst := r.InstanceSpace[preAccept.LeaderId][preAccept.Instance]

	if preAccept.Seq >= r.maxSeq {
		r.maxSeq = preAccept.Seq + 1
	}

	if inst != nil && (inst.Status == epaxosprotobuf.COMMITTED || inst.Status == epaxosprotobuf.ACCEPTED) {
		//reordered handling of commit/accept and pre-accept
		if inst.Cmds == nil {
			r.InstanceSpace[preAccept.LeaderId][preAccept.Instance].Cmds = preAccept.Command
			r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
		}

		r.recordCommands(preAccept.Command)
		r.sync()
		return nil
	}

	if preAccept.Instance >= r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance + 1
	}

	//update attributes for command
	seq, deps, changed := r.updateAttributes(preAccept.Command, preAccept.Seq, preAccept.Deps, preAccept.Replica, preAccept.Instance)

	uncommittedDeps := false
	for q := 0; q < r.N; q++ {
		if deps[q] > r.CommittedUpTo[q] {
			uncommittedDeps = true
			break
		}
	}
	status := epaxosprotobuf.PREACCEPTED_EQ
	if changed {
		status = epaxosprotobuf.PREACCEPTED
	}

	if inst != nil {
		if preAccept.Ballot < inst.ballot {
			panic("handlePreAccept: wrong ballot")
		} else {
			inst.Cmds = preAccept.Command
			inst.Seq = seq
			inst.Deps = deps
			inst.ballot = preAccept.Ballot
			inst.Status = status
		}
	} else {
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = &Instance{
			preAccept.Command,
			preAccept.Ballot,
			status,
			seq,
			deps,
			nil, 0, 0,
		}
	}

	r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)

	r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
	r.recordCommands(preAccept.Command)
	r.sync()

	if len(preAccept.Command) == 0 {
		//checkpoint
		//update latest checkpoint info
		r.latestCPReplica = preAccept.Replica
		r.latestCPInstance = preAccept.Instance

		//discard dependency hashtables
		r.clearHashtables()
	}

	if changed || uncommittedDeps || preAccept.Replica != preAccept.LeaderId || !isInitialBallot(preAccept.Ballot) {
		return &epaxosprotobuf.PreAcceptReply{
			preAccept.Replica,
			preAccept.Instance,
			epaxosprotobuf.PREACCEPTED,
			preAccept.Ballot,
			seq,
			deps,
			r.CommittedUpTo}

	} else {
		return &epaxosprotobuf.PreAcceptReply{Ok: epaxosprotobuf.PREACCEPTED_EQ}
	}
}

/**********************************************************************

                        PHASE 2

***********************************************************************/

func (r *Replica) AcceptRPC(ctx context.Context, a *epaxosprotobuf.Accept) (*epaxosprotobuf.AcceptReply, error) {
	reply := r.handleAccept(a)
	if reply == nil {
		return &epaxosprotobuf.AcceptReply{}, grpc.Errorf(codes.AlreadyExists, "%s", "instance already committed")
	}
	return reply, nil
}

func (r *Replica) handleAccept(accept *epaxosprotobuf.Accept) *epaxosprotobuf.AcceptReply {
	ghl.Lock()
	defer ghl.Unlock()

	inst := r.InstanceSpace[accept.LeaderId][accept.Instance]

	if accept.Seq >= r.maxSeq {
		r.maxSeq = accept.Seq + 1
	}

	if inst != nil && (inst.Status == epaxosprotobuf.COMMITTED || inst.Status == epaxosprotobuf.EXECUTED) {
		return nil
	}

	if accept.Instance >= r.crtInstance[accept.LeaderId] {
		r.crtInstance[accept.LeaderId] = accept.Instance + 1
	}

	if inst != nil {
		if accept.Ballot < inst.ballot {
			//r.replyAccept(accept.LeaderId, &epaxosprotobuf.AcceptReply{accept.Replica, accept.Instance, epaxosprotobuf.NONE, inst.ballot})
			return &epaxosprotobuf.AcceptReply{
				accept.Replica,
				accept.Instance,
				epaxosprotobuf.NONE,
				inst.ballot,
			}
		}
		inst.Status = epaxosprotobuf.ACCEPTED
		inst.Seq = accept.Seq
		inst.Deps = accept.Deps
	} else {
		r.InstanceSpace[accept.LeaderId][accept.Instance] = &Instance{
			nil,
			accept.Ballot,
			epaxosprotobuf.ACCEPTED,
			accept.Seq,
			accept.Deps,
			nil, 0, 0,
		}
		if accept.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = accept.Replica
			r.latestCPInstance = accept.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}

	r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
	r.sync()

	return &epaxosprotobuf.AcceptReply{
		accept.Replica,
		accept.Instance,
		epaxosprotobuf.PREACCEPTED,
		accept.Ballot,
	}
}

/**********************************************************************

                            COMMIT

***********************************************************************/

func (r *Replica) CommitRPC(cs epaxosprotobuf.CommitService_CommitRPCServer) error {
	for {
		csm, err := cs.Recv()
		if err == io.EOF {
			return cs.SendAndClose(&epaxosprotobuf.Empty{})
		}
		if err != nil {
			log.Fatalln("commit recv error:", err)
		}
		r.handleCommit(csm)
	}
	return nil
}

func (r *Replica) CommitShortRPC(cs epaxosprotobuf.CommitService_CommitShortRPCServer) error {
	for {
		csm, err := cs.Recv()
		if err == io.EOF {
			return cs.SendAndClose(&epaxosprotobuf.Empty{})
		}
		if err != nil {
			log.Fatalln("commit recv error:", err)
		}
		r.handleCommitShort(csm)
	}
	return nil
}

func (r *Replica) handleCommit(commit *epaxosprotobuf.Commit) {
	ghl.Lock()
	defer ghl.Unlock()

	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Seq >= r.maxSeq {
		r.maxSeq = commit.Seq + 1
	}

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil && len(commit.Command) == 0 {
			//someone committed a NO-OP, but we have proposals for this instance
			//try in a different instance
			for _, p := range inst.lb.clientProposals {
				r.ProposeChan <- p
			}
			inst.lb = nil
		}
		inst.Seq = commit.Seq
		inst.Deps = commit.Deps
		inst.Status = epaxosprotobuf.COMMITTED
	} else {
		r.InstanceSpace[commit.Replica][int(commit.Instance)] = &Instance{
			commit.Command,
			0,
			epaxosprotobuf.COMMITTED,
			commit.Seq,
			commit.Deps,
			nil,
			0,
			0,
		}
		r.updateConflicts(commit.Command, commit.Replica, commit.Instance, commit.Seq)

		if len(commit.Command) == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *epaxosprotobuf.CommitShort) {
	ghl.Lock()
	defer ghl.Unlock()

	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//try command in a different instance
			for _, p := range inst.lb.clientProposals {
				r.ProposeChan <- p
			}
			inst.lb = nil
		}
		inst.Seq = commit.Seq
		inst.Deps = commit.Deps
		inst.Status = epaxosprotobuf.COMMITTED
	} else {
		r.InstanceSpace[commit.Replica][commit.Instance] = &Instance{
			nil,
			0,
			epaxosprotobuf.COMMITTED,
			commit.Seq,
			commit.Deps,
			nil, 0, 0,
		}
		if commit.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
}
