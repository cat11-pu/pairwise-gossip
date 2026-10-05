"""Acceptance tests for the gossip membership core."""

import unittest

from gossip.core import (ALIVE, HEARTBEAT_MODULUS, SUSPECT, Message,
                         Simulation)


class GossipProtocolTests(unittest.TestCase):

    def test_published_rumor_reaches_every_node(self):
        sim = Simulation(["a", "b", "c"], seed=17, fanout=2, suspicion_timeout=50)
        rumor_id = sim.nodes["a"].publish("topic", "on")
        sim.run_until_quiet(max_rounds=30)
        for node_id, node in sim.nodes.items():
            self.assertIn(rumor_id, node.rumors, "%s did not receive the rumor" % node_id)
            self.assertEqual(node.rumors[rumor_id]["key"], "topic")
            self.assertEqual(node.rumors[rumor_id]["value"], "on")
        for entry in sim.network.sent_log:
            self.assertNotEqual(entry[1], entry[2],
                                "%s sent a message to itself" % entry[1])

    def test_versions_and_rumors_agree_after_convergence(self):
        sim = Simulation(["a", "b", "c", "d"], seed=11, fanout=2, suspicion_timeout=50)
        for node in sim.nodes.values():
            node.publish("key-" + node.node_id, "value-" + node.node_id)
        sim.run_until_quiet(max_rounds=60)
        expected_rumors = {"a:1", "b:1", "c:1", "d:1"}
        expected_version = {"a": 1, "b": 1, "c": 1, "d": 1}
        for node_id, node in sim.nodes.items():
            self.assertEqual(set(node.rumors), expected_rumors,
                             "%s has an incomplete rumor store" % node_id)
            self.assertEqual(dict(node.version), expected_version,
                             "%s has a stale version vector" % node_id)
        global_max = {}
        for node in sim.nodes.values():
            for origin, counter in node.version.items():
                global_max[origin] = max(global_max.get(origin, 0), counter)
        for node_id, node in sim.nodes.items():
            for origin, counter in node.version.items():
                self.assertLessEqual(counter, global_max[origin],
                                     "%s counts beyond the cluster maximum" % node_id)

    def test_unknown_sender_and_empty_payload_are_handled(self):
        sim = Simulation(["a", "b"], seed=19, fanout=1, suspicion_timeout=50)
        a = sim.nodes["a"]
        intruder = Message(
            "z-1", "z", "a", "rumor",
            {
                "version": {"z": 1},
                "members": [{"member_id": "z", "heartbeat": 1, "join_epoch": 1}],
                "rumors": {"z:1": {"origin": "z", "counter": 1, "key": "k", "value": "v"}},
            },
        )
        self.assertFalse(a.receive(intruder))
        self.assertEqual(a.stats["rejected"], 1)
        self.assertNotIn("z", a.members)
        self.assertEqual(a.rumors, {})
        self.assertTrue(a.receive(Message("b-1", "b", "a", "rumor", None)))
        self.assertTrue(a.receive(Message("b-2", "b", "a", "rumor",
                                          {"members": [{"heartbeat": 7}]})))
        self.assertIn("b", a.members)
        self.assertEqual(a.rumors, {})
        self.assertEqual(a.members["b"].join_epoch, 2)
        self.assertEqual(a.members["b"].heartbeat, 0)
        far_ahead = HEARTBEAT_MODULUS // 2 + 100
        a.receive(Message("b-3", "b", "a", "heartbeat",
                          {"members": [{"member_id": "b", "heartbeat": far_ahead,
                                        "join_epoch": 2, "status": "alive"}]}))
        self.assertEqual(a.members["b"].heartbeat, 0,
                         "an implausible heartbeat was accepted as progress")
        a.receive(Message("b-4", "b", "a", "heartbeat",
                          {"members": [{"member_id": "b", "heartbeat": 1,
                                        "join_epoch": 2, "status": "alive"}]}))
        self.assertEqual(a.members["b"].heartbeat, 1,
                         "a plain heartbeat was not accepted as progress")

    def test_round_targets_peers_once_and_never_itself(self):
        sim = Simulation(["a", "b", "c", "d"], seed=23, fanout=2, suspicion_timeout=50)
        a = sim.nodes["a"]
        a.publish("k", "v")
        a.tick()
        first_targets = [entry[2] for entry in sim.network.sent_log if entry[1] == "a"]
        self.assertEqual(len(first_targets), 2)
        self.assertEqual(sorted(first_targets), sorted(set(first_targets)))
        self.assertNotIn("a", first_targets, "a targeted itself")
        sim.step(4)
        seen = set()
        for tick, sender, recipient, kind, rumor_ids in sim.network.sent_log:
            for rumor_id in rumor_ids:
                marker = (tick, sender, recipient, rumor_id)
                self.assertNotIn(marker, seen,
                                 "%s pushed %s twice to %s in one round"
                                 % (sender, rumor_id, recipient))
                seen.add(marker)

    def test_rumor_relayed_by_two_peers_is_processed_once(self):
        sim = Simulation(["a", "b", "c"], seed=29, fanout=2, suspicion_timeout=50)
        rumor_id = sim.nodes["a"].publish("k", "v")
        sim.run_until_quiet(max_rounds=40)
        for node_id in ("b", "c"):
            node = sim.nodes[node_id]
            self.assertIn(rumor_id, node.rumors, "%s missed the rumor" % node_id)
            self.assertEqual(node.stats["accepted"], 1,
                             "%s processed the rumor again" % node_id)
            self.assertGreaterEqual(node.stats["duplicates"], 1,
                                    "%s never noticed the repeated delivery" % node_id)

    def test_no_traffic_after_every_peer_knows_everything(self):
        sim = Simulation(["a", "b", "c"], seed=31, fanout=2, suspicion_timeout=50)
        sim.nodes["a"].publish("one", "1")
        sim.nodes["b"].publish("two", "2")
        sim.run_until_quiet(max_rounds=30)
        self.assertEqual(sim.network.pending(), 0, "messages are still in flight")
        self.assertEqual(sim.pending_relays(), 0, "relays are still pending")
        pushed = {}
        for tick, sender, recipient, kind, rumor_ids in sim.network.sent_log:
            if kind != "rumor":
                continue
            for rumor_id in rumor_ids:
                marker = (sender, recipient, rumor_id)
                self.assertNotIn(marker, pushed,
                                 "%s pushed %s to %s again"
                                 % (sender, rumor_id, recipient))
                pushed[marker] = tick
        sent_before = sim.sent_total()
        sim.step(4)
        self.assertEqual(sim.sent_total(), sent_before,
                         "new messages appeared after the cluster was informed")
        self.assertEqual(sim.network.pending(), 0)

    def test_removed_member_is_not_restored_by_an_old_view(self):
        sim = Simulation(["a", "b", "c"], seed=37, fanout=2, suspicion_timeout=50)
        a = sim.nodes["a"]
        b = sim.nodes["b"]
        self.assertTrue(a.remove_member("b"))
        self.assertNotIn("b", a.members)
        stale_view = [{"member_id": "b", "heartbeat": 5,
                       "join_epoch": b.join_epoch, "status": "alive"}]
        a.merge_members(stale_view)
        self.assertNotIn("b", a.members, "an old view restored a removed member")
        rejoined = [{"member_id": "b", "heartbeat": 1,
                     "join_epoch": b.join_epoch + 1, "status": "alive"}]
        a.merge_members(rejoined)
        self.assertIn("b", a.members)
        self.assertEqual(a.members["b"].join_epoch, b.join_epoch + 1)

    def test_suspected_member_recovers_across_a_heartbeat_wrap(self):
        sim = Simulation(["a", "b"], seed=41, fanout=1, suspicion_timeout=4)
        a = sim.nodes["a"]
        b = sim.nodes["b"]
        b.heartbeat = HEARTBEAT_MODULUS - 2
        b.members["b"].heartbeat = b.heartbeat
        a.members["b"].heartbeat = b.heartbeat
        b.announce()
        sim.step()
        self.assertEqual(a.members["b"].heartbeat, HEARTBEAT_MODULUS - 1)
        self.assertEqual(a.members["b"].status, ALIVE)
        sim.step(5)
        self.assertEqual(a.members["b"].status, SUSPECT)
        b.announce()
        sim.step()
        self.assertEqual(a.members["b"].status, ALIVE,
                         "the refreshed member is still suspect")
        sim.step(3)
        self.assertEqual(a.members["b"].status, ALIVE,
                         "the refreshed member was declared dead")

    def test_anti_entropy_exchange_closes_gaps_on_both_sides(self):
        sim = Simulation(["a", "b"], seed=43, fanout=1, suspicion_timeout=50)
        a = sim.nodes["a"]
        b = sim.nodes["b"]
        left = a.publish("left", "1")
        right = b.publish("right", "2")
        b.sync_with("a")
        a.sync_with("b")
        for _ in range(2):
            sim.clock.advance(1)
            sim.deliver()
        self.assertIn(right, a.rumors, "a did not pull the newer rumor")
        self.assertIn(left, b.rumors, "b did not pull the newer rumor")
        self.assertEqual(a.rumors[right]["value"], "2")
        self.assertEqual(b.rumors[left]["value"], "1")


if __name__ == "__main__":
    unittest.main()
