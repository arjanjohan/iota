// Copyright (c) Mysten Labs, Inc.
// Modifications Copyright (c) 2025 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
mod test {
    use super::*;
    use crate::leader_scoring::ReputationScores;

    #[cfg(any())]
    mod smart_ancestor_selection_tests {
        #[tokio::test]
        async fn test_calculate_network_high_received_quorum_round() {
            telemetry_subscribers::init_for_testing();

            let (mut context, _key_pairs) = Context::new_for_test(4);
            context
                .protocol_config
                .set_consensus_round_prober_probe_accepted_rounds(false);
            let context = Arc::new(context);

            let scores = ReputationScores::new((1..=300).into(), vec![1, 2, 4, 3]);
            let mut ancestor_state_manager = AncestorStateManager::new(context.clone());
            ancestor_state_manager.set_propagation_scores(scores);

            // Quorum rounds are not set yet, so we should calculate a network
            // quorum round of 0 to start.
            let network_high_quorum_round =
                ancestor_state_manager.calculate_network_high_quorum_round();
            assert_eq!(network_high_quorum_round, 0);

            let received_quorum_rounds = vec![(100, 229), (225, 229), (229, 300), (229, 300)];
            let accepted_quorum_rounds = vec![(50, 229), (175, 229), (179, 229), (179, 300)];
            ancestor_state_manager.set_quorum_rounds_per_authority(
                received_quorum_rounds.clone(),
                accepted_quorum_rounds.clone(),
            );

            // When probe_accepted_rounds is false, should use received rounds
            let network_high_quorum_round =
                ancestor_state_manager.calculate_network_high_quorum_round();
            assert_eq!(network_high_quorum_round, 300);
        }

        #[tokio::test]
        async fn test_calculate_network_high_accepted_quorum_round() {
            telemetry_subscribers::init_for_testing();

            let (mut context, _key_pairs) = Context::new_for_test(4);
            context
                .protocol_config
                .set_consensus_round_prober_probe_accepted_rounds(true);
            let context = Arc::new(context);

            let scores = ReputationScores::new((1..=300).into(), vec![1, 2, 4, 3]);
            let mut ancestor_state_manager = AncestorStateManager::new(context.clone());
            ancestor_state_manager.set_propagation_scores(scores);

            // Quorum rounds are not set yet, so we should calculate a network
            // quorum round of 0 to start.
            let network_high_quorum_round =
                ancestor_state_manager.calculate_network_high_quorum_round();
            assert_eq!(network_high_quorum_round, 0);

            let received_quorum_rounds = vec![(100, 229), (225, 300), (229, 300), (229, 300)];
            let accepted_quorum_rounds = vec![(50, 229), (175, 229), (179, 229), (179, 300)];
            ancestor_state_manager.set_quorum_rounds_per_authority(
                received_quorum_rounds.clone(),
                accepted_quorum_rounds.clone(),
            );

            // When probe_accepted_rounds is true, should use accepted rounds
            let network_high_quorum_round =
                ancestor_state_manager.calculate_network_high_quorum_round();
            assert_eq!(network_high_quorum_round, 229);
        }

        // Test all state transitions with probe_accepted_rounds = true
        // Default all INCLUDE -> EXCLUDE
        // EXCLUDE -> INCLUDE (Blocked due to lock)
        // EXCLUDE -> INCLUDE (Pass due to lock expired)
        // INCLUDE -> EXCLUDE (Blocked due to lock)
        // INCLUDE -> EXCLUDE (Pass due to lock expired)
        #[tokio::test]
        async fn test_update_all_ancestor_state_using_accepted_rounds() {
            telemetry_subscribers::init_for_testing();
            let (mut context, _key_pairs) = Context::new_for_test(4);
            context
                .protocol_config
                .set_consensus_round_prober_probe_accepted_rounds(true);
            let context = Arc::new(context);

            let scores = ReputationScores::new((1..=300).into(), vec![1, 2, 4, 3]);
            let mut ancestor_state_manager = AncestorStateManager::new(context);
            ancestor_state_manager.set_propagation_scores(scores);

            let received_quorum_rounds = vec![(300, 400), (300, 400), (300, 400), (300, 400)];
            let accepted_quorum_rounds = vec![(225, 229), (225, 229), (229, 300), (229, 300)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Score threshold for exclude is (4 * 10) / 100 = 0
            // No ancestors should be excluded in with this threshold
            let state_map = ancestor_state_manager.get_ancestor_states();
            for state in state_map.iter() {
                assert_eq!(*state, AncestorState::Include);
            }

            let scores = ReputationScores::new((1..=300).into(), vec![10, 10, 100, 100]);
            ancestor_state_manager.set_propagation_scores(scores);
            ancestor_state_manager.update_all_ancestors_state();

            // Score threshold for exclude is (100 * 10) / 100 = 10
            // 2 authorities should be excluded in with this threshold
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if (0..=1).contains(&authority) {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            ancestor_state_manager.update_all_ancestors_state();

            // 2 authorities should still be excluded with these scores and no new
            // quorum round updates have been set to expire the locks.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if (0..=1).contains(&authority) {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            // Updating the quorum rounds will expire the lock as we only need 1
            // quorum round update for tests.
            let received_quorum_rounds = vec![(400, 500), (400, 500), (400, 500), (400, 500)];
            let accepted_quorum_rounds = vec![(229, 300), (225, 229), (229, 300), (229, 300)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Authority 0 should now be included again because high quorum round is
            // at the network high quorum round of 300. Authority 1's quorum round is
            // too low and will remain excluded.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if authority == 1 {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            let received_quorum_rounds = vec![(500, 600), (500, 600), (500, 600), (500, 600)];
            let accepted_quorum_rounds = vec![(229, 300), (229, 300), (229, 300), (229, 300)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Ancestor 1 can transition to the INCLUDE state. Ancestor 0 is still locked
            // in the INCLUDE state until a score update is performed which is why
            // even though the scores are still low it has not moved to the EXCLUDE
            // state.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for state in state_map.iter() {
                assert_eq!(*state, AncestorState::Include);
            }

            // Updating the scores will expire the lock as we only need 1 update for tests.
            let scores = ReputationScores::new((1..=300).into(), vec![100, 10, 100, 100]);
            ancestor_state_manager.set_propagation_scores(scores);
            ancestor_state_manager.update_all_ancestors_state();

            // Ancestor 1 can transition to EXCLUDE state now that the lock expired
            // and its scores are below the threshold.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if authority == 1 {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }
        }

        // Test all state transitions with probe_accepted_rounds = false
        // Default all INCLUDE -> EXCLUDE
        // EXCLUDE -> INCLUDE (Blocked due to lock)
        // EXCLUDE -> INCLUDE (Pass due to lock expired)
        // INCLUDE -> EXCLUDE (Blocked due to lock)
        // INCLUDE -> EXCLUDE (Pass due to lock expired)
        #[tokio::test]
        async fn test_update_all_ancestor_state_using_received_rounds() {
            telemetry_subscribers::init_for_testing();
            let (mut context, _key_pairs) = Context::new_for_test(4);
            context
                .protocol_config
                .set_consensus_round_prober_probe_accepted_rounds(false);
            let context = Arc::new(context);

            let scores = ReputationScores::new((1..=300).into(), vec![1, 2, 4, 3]);
            let mut ancestor_state_manager = AncestorStateManager::new(context);
            ancestor_state_manager.set_propagation_scores(scores);

            let received_quorum_rounds = vec![(225, 229), (225, 300), (229, 300), (229, 300)];
            let accepted_quorum_rounds = vec![(100, 150), (100, 150), (100, 150), (100, 150)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Score threshold for exclude is (4 * 10) / 100 = 0
            // No ancestors should be excluded in with this threshold
            let state_map = ancestor_state_manager.get_ancestor_states();
            for state in state_map.iter() {
                assert_eq!(*state, AncestorState::Include);
            }

            let scores = ReputationScores::new((1..=300).into(), vec![10, 10, 100, 100]);
            ancestor_state_manager.set_propagation_scores(scores);
            ancestor_state_manager.update_all_ancestors_state();

            // Score threshold for exclude is (100 * 10) / 100 = 10
            // 2 authorities should be excluded in with this threshold
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if (0..=1).contains(&authority) {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            ancestor_state_manager.update_all_ancestors_state();

            // 2 authorities should still be excluded with these scores and no new
            // quorum round updates have been set to expire the locks.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if (0..=1).contains(&authority) {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            // Updating the quorum rounds will expire the lock as we only need 1
            // quorum round update for tests.
            let received_quorum_rounds = vec![(229, 300), (225, 229), (229, 300), (229, 300)];
            let accepted_quorum_rounds = vec![(100, 150), (100, 150), (100, 150), (100, 150)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Authority 0 should now be included again because high quorum round is
            // at the network high quorum round of 300. Authority 1's quorum round is
            // too low and will remain excluded.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if authority == 1 {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }

            let received_quorum_rounds = vec![(229, 300), (229, 300), (229, 300), (229, 300)];
            let accepted_quorum_rounds = vec![(100, 150), (100, 150), (100, 150), (100, 150)];
            ancestor_state_manager
                .set_quorum_rounds_per_authority(received_quorum_rounds, accepted_quorum_rounds);
            ancestor_state_manager.update_all_ancestors_state();

            // Ancestor 1 can transition to the INCLUDE state. Ancestor 0 is still locked
            // in the INCLUDE state until a score update is performed which is why
            // even though the scores are still low it has not moved to the EXCLUDE
            // state.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for state in state_map.iter() {
                assert_eq!(*state, AncestorState::Include);
            }

            // Updating the scores will expire the lock as we only need 1 update for tests.
            let scores = ReputationScores::new((1..=300).into(), vec![100, 10, 100, 100]);
            ancestor_state_manager.set_propagation_scores(scores);
            ancestor_state_manager.update_all_ancestors_state();

            // Ancestor 1 can transition to EXCLUDE state now that the lock expired
            // and its scores are below the threshold.
            let state_map = ancestor_state_manager.get_ancestor_states();
            for (authority, state) in state_map.iter().enumerate() {
                if authority == 1 {
                    assert_eq!(*state, AncestorState::Exclude(10));
                } else {
                    assert_eq!(*state, AncestorState::Include);
                }
            }
        }
    }
}
