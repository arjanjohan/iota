// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2024 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod test {
    #[cfg(any())]
    mod round_prober_tests {
        use std::{collections::BTreeSet, sync::Arc, time::Duration};

        use async_trait::async_trait;
        use bytes::Bytes;
        use parking_lot::{Mutex, RwLock};
        use starfish_config::AuthorityIndex;

        use super::QuorumRound;
        use crate::{
            Round, TestBlock, VerifiedBlock,
            block::BlockRef,
            commit::{CertifiedCommits, CommitRange},
            context::Context,
            core_thread::{CoreError, CoreThreadDispatcher},
            dag_state::DagState,
            error::{ConsensusError, ConsensusResult},
            network::{BlockStream, NetworkClient},
            round_prober::{RoundProber, compute_quorum_round},
            storage::mem_store::MemStore,
        };

        struct FakeThreadDispatcher {
            highest_received_rounds: Vec<Round>,
            propagation_delay: Mutex<Round>,
            received_quorum_rounds: Mutex<Vec<QuorumRound>>,
            accepted_quorum_rounds: Mutex<Vec<QuorumRound>>,
        }

        impl FakeThreadDispatcher {
            fn new(highest_received_rounds: Vec<Round>) -> Self {
                Self {
                    highest_received_rounds,
                    propagation_delay: Mutex::new(0),
                    received_quorum_rounds: Mutex::new(Vec::new()),
                    accepted_quorum_rounds: Mutex::new(Vec::new()),
                }
            }

            fn propagation_delay(&self) -> Round {
                *self.propagation_delay.lock()
            }

            fn received_quorum_rounds(&self) -> Vec<QuorumRound> {
                self.received_quorum_rounds.lock().clone()
            }

            fn accepted_quorum_rounds(&self) -> Vec<QuorumRound> {
                self.accepted_quorum_rounds.lock().clone()
            }
        }

        #[async_trait]
        impl CoreThreadDispatcher for FakeThreadDispatcher {
            async fn add_blocks(
                &self,
                _blocks: Vec<VerifiedBlock>,
            ) -> Result<BTreeSet<BlockRef>, CoreError> {
                unimplemented!()
            }

            async fn add_certified_commits(
                &self,
                _commits: CertifiedCommits,
            ) -> Result<BTreeSet<BlockRef>, CoreError> {
                unimplemented!()
            }

        async fn new_block(&self, _round: Round, _force: bool) -> Result<(), CoreError> {
            unimplemented!()
        }

            async fn get_missing_blocks(&self) -> Result<BTreeSet<BlockRef>, CoreError> {
                unimplemented!()
            }

            fn set_subscriber_exists(&self, _exists: bool) -> Result<(), CoreError> {
                unimplemented!()
            }

            fn set_propagation_delay_and_quorum_rounds(
                &self,
                delay: Round,
                received_quorum_rounds: Vec<QuorumRound>,
                accepted_quorum_rounds: Vec<QuorumRound>,
            ) -> Result<(), CoreError> {
                let mut received_quorum_round_per_authority = self.received_quorum_rounds.lock();
                *received_quorum_round_per_authority = received_quorum_rounds;
                let mut accepted_quorum_round_per_authority = self.accepted_quorum_rounds.lock();
                *accepted_quorum_round_per_authority = accepted_quorum_rounds;
                let mut propagation_delay = self.propagation_delay.lock();
                *propagation_delay = delay;
                Ok(())
            }

            fn set_last_known_proposed_round(&self, _round: Round) -> Result<(), CoreError> {
                unimplemented!()
            }

            fn highest_received_rounds(&self) -> Vec<Round> {
                self.highest_received_rounds.clone()
            }
        }

        struct FakeNetworkClient {
            highest_received_rounds: Vec<Vec<Round>>,
            highest_accepted_rounds: Vec<Vec<Round>>,
        }

        impl FakeNetworkClient {
            fn new(
                highest_received_rounds: Vec<Vec<Round>>,
                highest_accepted_rounds: Vec<Vec<Round>>,
            ) -> Self {
                Self {
                    highest_received_rounds,
                    highest_accepted_rounds,
                }
            }
        }

        #[async_trait]
        #[async_trait::async_trait]
        impl NetworkClient for FakeNetworkClient {
            const SUPPORT_STREAMING: bool = true;

            async fn send_block(
                &self,
                _peer: AuthorityIndex,
                _serialized_block: &VerifiedBlock,
                _timeout: Duration,
            ) -> ConsensusResult<()> {
                unimplemented!("Unimplemented")
            }

            async fn subscribe_blocks(
                &self,
                _peer: AuthorityIndex,
                _last_received: Round,
                _timeout: Duration,
            ) -> ConsensusResult<BlockStream> {
                unimplemented!("Unimplemented")
            }

            async fn fetch_blocks(
                &self,
                _peer: AuthorityIndex,
                _block_refs: Vec<BlockRef>,
                _highest_accepted_rounds: Vec<Round>,
                _timeout: Duration,
            ) -> ConsensusResult<Vec<Bytes>> {
                unimplemented!("Unimplemented")
            }

            async fn fetch_commits(
                &self,
                _peer: AuthorityIndex,
                _commit_range: CommitRange,
                _timeout: Duration,
            ) -> ConsensusResult<(Vec<Bytes>, Vec<Bytes>)> {
                unimplemented!("Unimplemented")
            }

            async fn fetch_latest_blocks(
                &self,
                _peer: AuthorityIndex,
                _authorities: Vec<AuthorityIndex>,
                _timeout: Duration,
            ) -> ConsensusResult<Vec<Bytes>> {
                unimplemented!("Unimplemented")
            }

            async fn get_latest_rounds(
                &self,
                peer: AuthorityIndex,
                _timeout: Duration,
            ) -> ConsensusResult<(Vec<Round>, Vec<Round>)> {
                let received_rounds = self.highest_received_rounds[peer].clone();
                let accepted_rounds = self.highest_accepted_rounds[peer].clone();
                if received_rounds.is_empty() && accepted_rounds.is_empty() {
                    Err(ConsensusError::NetworkRequestTimeout("test".to_string()))
                } else {
                    Ok((received_rounds, accepted_rounds))
                }
            }
        }

        #[tokio::test]
        async fn test_round_prober() {
            const NUM_AUTHORITIES: usize = 7;
            let context = Arc::new(Context::new_for_test(NUM_AUTHORITIES).0);
            let core_thread_dispatcher = Arc::new(FakeThreadDispatcher::new(vec![
                110, 120, 130, 140, 150, 160, 170,
            ]));
            let store = Arc::new(MemStore::new());
            let dag_state = Arc::new(RwLock::new(DagState::new(context.clone(), store)));
            // Have some peers return error or incorrect number of rounds.
            let network_client = Arc::new(FakeNetworkClient::new(
                vec![
                    vec![],
                    vec![109, 121, 131, 0, 151, 161, 171],
                    vec![101, 0, 103, 104, 105, 166, 107],
                    vec![],
                    vec![100, 102, 133, 0, 155, 106, 177],
                    vec![105, 115, 103, 0, 125, 126, 127],
                    vec![10, 20, 30, 40, 50, 60],
                ], // highest_received_rounds
                vec![
                    vec![],
                    vec![0, 121, 131, 0, 151, 161, 171],
                    vec![1, 0, 103, 104, 105, 166, 107],
                    vec![],
                    vec![0, 102, 133, 0, 155, 106, 177],
                    vec![1, 115, 103, 0, 125, 126, 127],
                    vec![1, 20, 30, 40, 50, 60],
                ], // highest_accepted_rounds
            ));
            let prober = RoundProber::new(
                context.clone(),
                core_thread_dispatcher.clone(),
                dag_state.clone(),
                network_client.clone(),
            );

            // Create test blocks for each authority with incrementing rounds starting at
            // 110
            let blocks = (0..NUM_AUTHORITIES)
                .map(|authority| {
                    let round = 110 + (authority as u32 * 10);
                    VerifiedBlock::new_for_test(TestBlock::new(round, authority as u32).build())
                })
                .collect::<Vec<_>>();

            dag_state.write().accept_blocks(blocks);

            // Compute quorum rounds and propagation delay based on last proposed round =
            // 110, and highest received rounds:
            // 110, 120, 130, 140, 150, 160, 170,
            // 109, 121, 131, 0,   151, 161, 171,
            // 101, 0,   103, 104, 105, 166, 107,
            // 0,   0,   0,   0,   0,   0,   0,
            // 100, 102, 133, 0,   155, 106, 177,
            // 105, 115, 103, 0,   125, 126, 127,
            // 0,   0,   0,   0,   0,   0,   0,

            let (received_quorum_rounds, accepted_quorum_rounds, propagation_delay) =
                prober.probe().await;

            assert_eq!(
                received_quorum_rounds,
                vec![
                    (100, 105),
                    (0, 115),
                    (103, 130),
                    (0, 0),
                    (105, 150),
                    (106, 160),
                    (107, 170)
                ]
            );

            assert_eq!(
                core_thread_dispatcher.received_quorum_rounds(),
                vec![
                    (100, 105),
                    (0, 115),
                    (103, 130),
                    (0, 0),
                    (105, 150),
                    (106, 160),
                    (107, 170)
                ]
            );
            // 110 - 100 = 10
            assert_eq!(propagation_delay, 10);
            assert_eq!(core_thread_dispatcher.propagation_delay(), 10);

            assert_eq!(
                accepted_quorum_rounds,
                vec![
                    (0, 1),
                    (0, 115),
                    (103, 130),
                    (0, 0),
                    (105, 150),
                    (106, 160),
                    (107, 170)
                ]
            );

            assert_eq!(
                core_thread_dispatcher.accepted_quorum_rounds(),
                vec![
                    (0, 1),
                    (0, 115),
                    (103, 130),
                    (0, 0),
                    (105, 150),
                    (106, 160),
                    (107, 170)
                ]
            );
        }

        #[tokio::test]
        async fn test_compute_quorum_round() {
            let (context, _) = Context::new_for_test(4);

            // Observe latest rounds from peers.
            let highest_received_rounds = vec![
                vec![10, 11, 12, 13],
                vec![5, 2, 7, 4],
                vec![0, 0, 0, 0],
                vec![3, 4, 5, 6],
            ];

            let round = compute_quorum_round(
                &context.committee,
                AuthorityIndex::new_for_test(0),
                &highest_received_rounds,
            );
            assert_eq!(round, (3, 5));

            let round = compute_quorum_round(
                &context.committee,
                AuthorityIndex::new_for_test(1),
                &highest_received_rounds,
            );
            assert_eq!(round, (2, 4));

            let round = compute_quorum_round(
                &context.committee,
                AuthorityIndex::new_for_test(2),
                &highest_received_rounds,
            );
            assert_eq!(round, (5, 7));

            let round = compute_quorum_round(
                &context.committee,
                AuthorityIndex::new_for_test(3),
                &highest_received_rounds,
            );
            assert_eq!(round, (4, 6));
        }
    }
}
