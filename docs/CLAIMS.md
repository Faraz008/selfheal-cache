# Claims and where they are proven

| # | Requirement | Proof (tests) | Proof (recorded output) |
|---|---|---|---|
| 1 | Multiple independent nodes form one logical cache | `tests/test_cluster.py::test_write_via_any_node_read_via_any_node`, `tests/test_cluster.py::test_all_nodes_agree_on_placement` | `docs/img/chaos_eventual.png` (5 containers) |
| 2 | Consistent hashing, minimal reshuffle | `tests/test_ring.py::test_adding_node_moves_about_one_over_n`, `tests/test_ring.py::test_removing_node_only_moves_its_keys`, `tests/test_ring.py::test_diff_agrees_with_per_key_lookup` | `docs/img/key_movement.png`, `docs/RING_RESULTS.txt` |
| 3 | Every key on ≥1 additional node; reads survive primary loss | `tests/test_cluster.py::test_every_key_lives_on_exactly_its_preference_list`, `tests/test_cluster.py::test_read_fails_over_when_primary_is_down`, `tests/test_failover.py::test_read_survives_dead_replica_before_detection` | |
| 4 | Automatic failure detection, no manual action | `tests/test_membership_cluster.py::test_kill_is_detected_by_every_node`, `tests/test_membership_cluster.py::test_indirect_probe_prevents_false_suspicion`, `tests/test_membership_cluster.py::test_false_suspicion_is_refuted` | `detect_s` in `docs/BENCHMARKS.md` |
| 5 | Traffic rerouted automatically | `tests/test_failover.py::test_sloppy_quorum_write_succeeds_with_dead_replica`, `tests/test_failover.py::test_hint_replays_when_owner_returns` | |
| 6 | Join / return rebalances with zero downtime | `tests/test_rebalance_load.py::test_add_node_under_load_fails_nothing_and_loses_nothing`, `tests/test_leave_cleanup.py::test_graceful_leave_hands_data_to_new_owners`, `tests/test_rebalance.py::test_restarted_node_catches_up_from_peers` | `grow` / `shrink` rows in `docs/img/chaos_eventual.png` |
| 7 | TTL honored consistently across replicas | `tests/test_ttl_paths.py::test_expire_at_survives_every_path`, `tests/test_clock_skew.py::test_restarted_replica_keeps_the_original_deadline`, `tests/test_clock_skew.py::test_ttl_expiry_is_consistent_only_to_within_the_clock_skew` | |
| 8 | Live kill mid-traffic: zero failed requests beyond one retry | `tests/test_client.py::test_client_survives_node_death_mid_traffic` | `kill_one`, `pause_one` rows in `docs/img/chaos_eventual.png`, `docs/demo.cast` |
| S1 | LRU/LFU eviction under memory pressure | `tests/test_evict.py::test_lru_evicts_the_coldest_key`, `tests/test_evict.py::test_lfu_survives_a_scan_that_flushes_lru`, `tests/test_evict_cluster.py::test_cap_is_local_and_data_stays_readable` | `docs/img/eviction_hit_rate.png` |
| S2 | Quorum mode as stricter alternative | `tests/test_quorum.py::test_eventual_mode_serves_a_stale_read`, `tests/test_quorum.py::test_quorum_mode_never_serves_that_stale_read`, `tests/test_quorum.py::test_quorum_write_fails_instead_of_going_sloppy` | `docs/PARTITIONS.md`, `docs/img/chaos_quorum.png` |

## Things this project does NOT claim

See "Known limits" in `docs/DESIGN.md`.