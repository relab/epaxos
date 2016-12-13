package epaxos

import erpc "github.com/relab/epaxos/epaxosprotobuf"

type EPaxosQSpec struct {
	q int
}

func (eqs *EPaxosQSpec) AcceptRPCQF(replies []*erpc.AcceptReply) (*erpc.AcceptReply, bool) {
	ok := 0
	for _, reply := range replies {
		if reply.Ok == erpc.NONE {
			return reply, true
		}
		ok++
		if ok == eqs.q {
			return reply, true
		}
	}
	return nil, false
}

func (eqs *EPaxosQSpec) PreAcceptRPCQF(replies []*erpc.PreAcceptReply) (*erpc.PreAcceptReply, bool) {
	// Avoid slice allocation.
	// 7 has to be adjusted if we want to run with n>7.
	var notEqIndices [7]int

	// Counters.
	var numOk, numNotEq int

	for i, reply := range replies {
		switch reply.Ok {
		case erpc.NONE:
			// Return the first NACK we see.
			return reply, true
		case erpc.PREACCEPTED_EQ:
			numOk++
		case erpc.PREACCEPTED:
			notEqIndices[numNotEq] = i
			numNotEq++
		}

		if numOk+numNotEq < eqs.q {
			// No quorum.
			continue
		}
		if numNotEq == 0 {
			// Quourm of OK replies.
			return reply, true
		}

		// fner: The first NOTEQ reply we have.
		fner := replies[notEqIndices[0]]

		if numNotEq == 1 {
			// Only one NOTEQ reply.
			// Nothing to merge; return it.
			return fner, true
		}

		// We have a quourm but need to merge
		// the NOTEQ replies.
		for j := 1; j < numNotEq; j++ {
			ner := replies[notEqIndices[j]]
			if ner.Seq > fner.Seq {
				fner.Seq = ner.Seq
			}
			for k := 0; k < len(ner.Deps); k++ {
				if ner.Deps[k] > fner.Deps[k] {
					fner.Deps[k] = ner.Deps[k]

				}
				if ner.CommittedDeps[k] > fner.CommittedDeps[k] {
					fner.CommittedDeps[k] = ner.CommittedDeps[k]
				}
			}
		}

		return fner, true
	}

	return nil, false
}
