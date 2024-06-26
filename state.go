/*
Copyright (C) THL A29 Limited, a Tencent company. All rights reserved.

SPDX-License-Identifier: Apache-2.0
*/

package tbft

import (
	consensuspb "chainmaker.org/chainmaker/pb-go/v2/consensus"
	tbftpb "chainmaker.org/chainmaker/pb-go/v2/consensus/tbft"
	"chainmaker.org/chainmaker/protocol/v2"
	"sync"
)

// ConsensusState represents the consensus state of the node
type ConsensusState struct {
	logger protocol.Logger
	// node id
	Id string
	// current height
	Height uint64
	// current round
	Round int32
	// current step
	Step tbftpb.Step

	// proposal
	Proposal *TBFTProposal
	ModifyProposal *TBFTProposal
	// verifing proposal
	VerifingProposal *TBFTProposal
	LockedRound      int32
	// locked proposal
	LockedProposal *tbftpb.Proposal
	ValidRound     int32
	// valid proposal
	ValidProposal      *tbftpb.Proposal
	heightRoundVoteSet *heightRoundVoteSet
}
type ModifyRequestState struct {
	logger protocol.Logger
	// node id
	Id string
	// current height
	ModifyBlockHeight uint64 //初始化为0
	// current round
	ModifyBlockRound int32 //初始化为0
	// current step
	ModifyConsensusStep tbftpb.Step //初始化为NEW_HEIGHT
	// proposal
	ModifyHeader *TBFTProposal //初始化为nil
	// verifing proposal
	VerifingModifyProposal *TBFTProposal //初始化为nil
	LockedModifyRound      int32
	//// locked proposal
	LockedModifyProposal *tbftpb.Proposal
	ValidModifyRound     int32
	//// valid proposal
	ValidModifyProposal      *tbftpb.Proposal
	modifyHeightRoundVoteSet *heightRoundVoteSet
}
// ConsensusFutureMsg represents the consensus msg of future
//
type ConsensusFutureMsg struct {
	Proposal           map[int32]*tbftpb.Proposal
	heightRoundVoteSet *heightRoundVoteSet
}

// ConsensusFutureMsgCache  cache future consensus msg
//
type ConsensusFutureMsgCache struct {
	logger          protocol.Logger
	size            uint64
	consensusHeight uint64
	cache           map[uint64]*ConsensusFutureMsg
}
type ModifyFutureMsgCache struct {
	logger          protocol.Logger
	size            uint64
	consensusHeight uint64
	cache           map[uint64]*ConsensusFutureMsg
}

//
// ConsensusFutureMsgCache
// @Description: Create a new future Msg cache
// @param size
// @return *ConsensusFutureMsgCache
//
func newConsensusFutureMsgCache(logger protocol.Logger, size, height uint64) *ConsensusFutureMsgCache {
	return &ConsensusFutureMsgCache{
		logger:          logger,
		size:            size,
		consensusHeight: height,
		cache:           make(map[uint64]*ConsensusFutureMsg, size),
	}
}
func newModifyFutureMsgCache (logger protocol.Logger, size, height uint64) *ModifyFutureMsgCache {
	return &ModifyFutureMsgCache{
		logger:          logger,
		size:            size,
		consensusHeight: height,
		cache:           make(map[uint64]*ConsensusFutureMsg, size),
	}
}

// NewConsensusFutureMsg creates a new future msg instance
func NewConsensusFutureMsg(logger protocol.Logger, height uint64, round int32,
	validators *validatorSet) *ConsensusFutureMsg {
	cs := &ConsensusFutureMsg{Proposal: make(map[int32]*tbftpb.Proposal)}
	cs.heightRoundVoteSet = newHeightRoundVoteSet(logger, height, round, validators)
	return cs
}

// updateConsensusHeight update height of consensus
func (cfc *ConsensusFutureMsgCache) updateConsensusHeight(height uint64) {
	cfc.consensusHeight = height

	// delete the cache
	cfc.gc()
}

// addFutureProposal add future proposal in cache
func (cfc *ConsensusFutureMsgCache) addFutureProposal(validators *validatorSet, proposal *tbftpb.Proposal) {
	// cache future proposal
	// we cahce : consensusHeight <= futureMsg.height <=  consensusHeight + size
	if cfc.consensusHeight+cfc.size < proposal.Height || proposal.Height < cfc.consensusHeight {
		return
	}
	if _, ok := cfc.cache[proposal.Height]; !ok {
		cs := NewConsensusFutureMsg(cfc.logger, proposal.Height, proposal.Round, validators)
		cfc.cache[proposal.Height] = cs
	}

	cfc.cache[proposal.Height].Proposal[proposal.Round] = proposal
	cfc.logger.Debugf("addFutureProposal proposal is [%s/%d/%d] %x", proposal.Voter, proposal.Height,
		proposal.Round, proposal.Block.Hash())
}
// addFutureProposal add future proposal in cache
func (mfc *ModifyFutureMsgCache) addFutureModifyProposal(validators *validatorSet, proposal *tbftpb.Proposal) {
	// cache future proposal
	// we cahce : consensusHeight <= futureMsg.height <=  consensusHeight + size
	if mfc.consensusHeight+mfc.size < proposal.Height {
		return
	}

	if _, ok := mfc.cache[proposal.Height]; !ok {
		mfc.logger.Infof("xqh 在ModifyFutureMsgCache中， cfc.cache[proposal.Height]不OK!!")
		cs := NewConsensusFutureMsg(mfc.logger, proposal.Height, proposal.Round, validators)
		mfc.cache[proposal.Height] = cs
	}

	mfc.cache[proposal.Height].Proposal[proposal.Round] = proposal
	mfc.logger.Debugf("xqh addModifyFutureProposal proposal is [%s/%d/%d] %x", proposal.Voter, proposal.Height,
		proposal.Round, proposal.Block.Hash())
}
// addFutureVote add future vote in cache
func (cfc *ConsensusFutureMsgCache) addFutureVote(validators *validatorSet, vote *tbftpb.Vote) {
	// cache future vote
	// we cahce : consensusHeight < futureMsg.height <=  consensusHeight + size
	if cfc.consensusHeight+cfc.size < vote.Height || vote.Height <= cfc.consensusHeight {
		return
	}
	if _, ok := cfc.cache[vote.Height]; !ok {
		cs := NewConsensusFutureMsg(cfc.logger, vote.Height, vote.Round, validators)
		cfc.cache[vote.Height] = cs
	}

	_, err := cfc.cache[vote.Height].heightRoundVoteSet.addVote(vote)
	if err != nil {
		cfc.logger.Debugf("addFutureVote addVote %v,  err: %v", vote, err)
	}
}
// addFutureVote add future vote in cache
func (mfc *ModifyFutureMsgCache) addModifyFutureVote(validators *validatorSet, vote *tbftpb.Vote) {
	// cache future vote
	// we cahce : consensusHeight < futureMsg.height <=  consensusHeight + size
	if mfc.consensusHeight+mfc.size < vote.Height  {
		return
	}
	if _, ok := mfc.cache[vote.Height]; !ok {
		cs := NewConsensusFutureMsg(mfc.logger, vote.Height, vote.Round, validators)
		mfc.cache[vote.Height] = cs
	}

	_, err := mfc.cache[vote.Height].heightRoundVoteSet.addVote(vote)
	if err != nil {
		mfc.logger.Debugf("addFutureVote addVote %v,  err: %v", vote, err)
	}
}

// getConsensusFutureProposal get future proposal in cache
func (cfc *ConsensusFutureMsgCache) getConsensusFutureProposal(height uint64, round int32) *tbftpb.Proposal {
	if state, ok := cfc.cache[height]; ok {
		return state.Proposal[round]
	}

	return nil
}

// getConsensusFutureProposal get future vote in cache
func (cfc *ConsensusFutureMsgCache) getConsensusFutureVote(height uint64, round int32) *roundVoteSet {
	if state, ok := cfc.cache[height]; ok {
		return state.heightRoundVoteSet.getRoundVoteSet(round)
	}

	return nil
}

//
// gc
// @Description: Delete the cache before the consensus height
// @receiver cache
// @param height
//
func (cfc *ConsensusFutureMsgCache) gc() {
	// delete every 10 heights
	if cfc.consensusHeight%10 != 0 {
		return
	}

	// delete the cache before the consensus height
	for k := range cfc.cache {
		cfc.logger.Debugf("futureCahce delete ,gc params: %d,%d,%d", k, cfc.size, cfc.consensusHeight)
		if k < cfc.consensusHeight {
			delete(cfc.cache, k)
		}
	}
}

// NewConsensusState creates a new ConsensusState instance
func NewConsensusState(logger protocol.Logger, id string) *ConsensusState {
	cs := &ConsensusState{
		logger: logger,
		Id:     id,
	}
	return cs
}
func NewModifyRequestState(logger protocol.Logger, id string) *ModifyRequestState {
	mr := &ModifyRequestState{
		logger: logger,
		Id:     id,
	}
	return mr
}
// toProto serializes the ConsensusState instance
func (cs *ConsensusState) toProto() *tbftpb.ConsensusState {
	if cs == nil {
		return nil
	}
	csProto := &tbftpb.ConsensusState{
		Id:                 cs.Id,
		Height:             cs.Height,
		Round:              cs.Round,
		Step:               cs.Step,
		Proposal:           cs.Proposal.PbMsg,
		VerifingProposal:   cs.VerifingProposal.PbMsg,
		HeightRoundVoteSet: cs.heightRoundVoteSet.ToProto(),
	}
	return csProto
}

//
// consensusStateCache
// @Description: Cache historical consensus state
//
type consensusStateCache struct {
	sync.Mutex
	size  uint64
	cache map[uint64]*ConsensusState
}
type modifyStateCache struct {
	sync.Mutex
	size  uint64
	cache map[uint64]*ModifyRequestState
}
//
// newConsensusStateCache
// @Description: Create a new state cache
// @param size
// @return *consensusStateCache
//
func newConsensusStateCache(size uint64) *consensusStateCache {
	return &consensusStateCache{
		size:  size,
		cache: make(map[uint64]*ConsensusState, size),
	}
}
func newModifyStateCache(size uint64) *modifyStateCache {
	return &modifyStateCache{
		size:  size,
		cache: make(map[uint64]*ModifyRequestState, size),
	}
}

//
// addConsensusState
// @Description: Add a new state to the cache
// @receiver cache
// @param state
//
func (cache *consensusStateCache) addConsensusState(state *ConsensusState) {
	if state == nil || state.Height <= 0 {
		return
	}

	cache.Lock()
	defer cache.Unlock()

	cache.cache[state.Height] = state
	cache.gc(state.Height)
}
func (cache *modifyStateCache) addModifyRequestState(state *ModifyRequestState) {
	if state == nil || state.ModifyBlockHeight <= 0 {
		return
	}
	cache.Lock()
	defer cache.Unlock()
	cache.cache[state.ModifyBlockHeight] = state
	cache.gc(state.ModifyBlockHeight)
}
//
// getConsensusState
// @Description: Get the desired consensus state from the cache, and return nil if it doesn't exist
// @receiver cache
// @param height
// @return *ConsensusState
//
func (cache *consensusStateCache) getConsensusState(height uint64) *ConsensusState {
	cache.Lock()
	defer cache.Unlock()

	if state, ok := cache.cache[height]; ok {
		return state
	}

	return nil
}

//
// gc
// @Description: Delete too many caches, triggered every time a new state is added to the cache
// @receiver cache
// @param height
//
func (cache *consensusStateCache) gc(height uint64) {
	for k := range cache.cache {
		//if k < (height - cache.size) {
		cache.cache[k].logger.Debugf("state delete ,gc params: %d,%d,%d", k, cache.size, height)
		if (k + cache.size) <= height {
			delete(cache.cache, k)
		}
	}
}
func (cache *modifyStateCache) gc(height uint64) {
	for k := range cache.cache {
		//if k < (height - cache.size) {
		cache.cache[k].logger.Debugf("state delete ,gc params: %d,%d,%d", k, cache.size, height)
		if (k + cache.size) <= height {
			delete(cache.cache, k)
		}
	}
}

type proposedProposal struct {
	proposedBlock *consensuspb.ProposalBlock
	qc            []*tbftpb.Vote
}
