"""Deterministic in-memory simulation of a gossip membership protocol.

Everything stays in memory: time comes from SimClock, messages travel through
MessageNetwork and every random decision comes from an injectable
SeededRandom instance, so a run with a fixed seed is reproducible.

A GossipNode keeps

  * a member table with heartbeat, join epoch and failure state,
  * a version vector counting the rumours known per origin,
  * a rumour store with the payloads that are being spread,
  * relay bookkeeping recording which peers were already told.

Rounds are driven by tick(): pending rumours are pushed to a bounded fan-out
of peers and the failure detector updates the member states. Anti entropy is
an explicit operation: sync_with(peer) asks a peer to close the gaps in this
node's knowledge.
"""

import random

ALIVE = "alive"
SUSPECT = "suspect"
DEAD = "dead"

DEFAULT_FANOUT = 2
DEFAULT_SUSPICION_TIMEOUT = 6
HEARTBEAT_MODULUS = 1 << 16


def rumor_id_for(origin, counter):
    """Stable identifier of a rumour."""
    return "%s:%d" % (origin, counter)


def heartbeat_newer(candidate, current):
    """Return True when candidate is fresher than current."""
    return candidate > current


class SimClock:
    """Manually advanced virtual clock."""

    def __init__(self, start=0):
        self._now = int(start)

    def now(self):
        return self._now

    def advance(self, ticks=1):
        self._now += int(ticks)
        return self._now


class SeededRandom:
    """Injectable deterministic random source."""

    def __init__(self, seed=0):
        self.seed = seed
        self._rng = random.Random(seed)

    def sample(self, population, k):
        items = list(population)
        if k >= len(items):
            return items
        return self._rng.sample(items, k)


class Message:
    """One in-flight protocol message."""

    def __init__(self, msg_id, sender, recipient, kind, payload):
        self.msg_id = msg_id
        self.sender = sender
        self.recipient = recipient
        self.kind = kind
        self.payload = payload

    def rumor_ids(self):
        return tuple(sorted((self.payload or {}).get("rumors", {})))


class MessageNetwork:
    """In-memory message queue with a fixed delivery latency."""

    def __init__(self, clock, latency=1):
        self.clock = clock
        self.latency = latency
        self.queue = []
        self.sent_log = []
        self.deliveries = []
        self._seq = 0

    def send(self, message):
        self._seq += 1
        self.queue.append((self.clock.now() + self.latency, self._seq, message))
        self.sent_log.append((self.clock.now(), message.sender, message.recipient,
                              message.kind, message.rumor_ids()))
        return message

    def pending(self):
        return len(self.queue)

    def deliver_due(self, nodes):
        now = self.clock.now()
        due = [item for item in self.queue if item[0] <= now]
        self.queue = [item for item in self.queue if item[0] > now]
        due.sort(key=lambda item: item[1])
        delivered = 0
        for _, _, message in due:
            target = nodes.get(message.recipient)
            if target is None:
                continue
            self.deliveries.append((now, message.sender, message.recipient,
                                    message.kind, message.rumor_ids()))
            target.receive(message)
            delivered += 1
        return delivered


class MemberState:
    """What a node knows about one cluster member."""

    def __init__(self, member_id, heartbeat=0, join_epoch=0, status=ALIVE, last_seen=0):
        self.member_id = member_id
        self.heartbeat = heartbeat
        self.join_epoch = join_epoch
        self.status = status
        self.last_seen = last_seen
        self.suspect_deadline = None

    def as_dict(self):
        return {
            "member_id": self.member_id,
            "heartbeat": self.heartbeat,
            "join_epoch": self.join_epoch,
            "status": self.status,
        }


class GossipNode:
    """One participant of the gossip cluster."""

    def __init__(self, node_id, network, clock, random_source, fanout=DEFAULT_FANOUT,
                 suspicion_timeout=DEFAULT_SUSPICION_TIMEOUT, heartbeat=0, join_epoch=0):
        self.node_id = node_id
        self.network = network
        self.clock = clock
        self.random_source = random_source
        self.fanout = fanout
        self.suspicion_timeout = suspicion_timeout
        self.heartbeat = heartbeat
        self.join_epoch = join_epoch

        now = clock.now()
        self.members = {node_id: MemberState(node_id, heartbeat=heartbeat,
                                             join_epoch=join_epoch, last_seen=now)}
        self.version = {node_id: 0}
        self.rumors = {}
        self.seen = set()
        self.relayed = {}
        self.tombstones = {}
        self.message_counter = 0
        self.stats = {"sent": 0, "received": 0, "accepted": 0, "duplicates": 0,
                      "rejected": 0}

    # ------------------------------------------------------------------
    # local operations
    # ------------------------------------------------------------------
    def publish(self, key, value):
        """Create a rumour owned by this node and return its id."""
        counter = self.version.get(self.node_id, 0) + 1
        self.version[self.node_id] = counter
        rumor_id = rumor_id_for(self.node_id, counter)
        rumor = {"origin": self.node_id, "counter": counter, "key": key, "value": value}
        self.rumors[rumor_id] = rumor
        self.seen.add(self._dedup_key(rumor_id, self.node_id))
        self.relayed[rumor_id] = {"rumor_id": rumor_id, "peers": set()}
        return rumor_id

    def announce(self, heartbeat=None):
        """Refresh this node's heartbeat and broadcast it to every peer."""
        if heartbeat is None:
            heartbeat = (self.heartbeat + 1) % HEARTBEAT_MODULUS
        self.heartbeat = heartbeat
        own = self.members[self.node_id]
        own.heartbeat = heartbeat
        own.status = ALIVE
        own.last_seen = self.clock.now()
        for peer_id in self.peer_ids():
            self._send(peer_id, "heartbeat", self._payload())
        return heartbeat

    def remove_member(self, member_id):
        """Drop a member locally and remember the tombstone."""
        member = self.members.get(member_id)
        if member is None or member_id == self.node_id:
            return False
        self.tombstones[member_id] = member.join_epoch
        del self.members[member_id]
        return True

    def sync_with(self, peer_id):
        """Start an anti entropy exchange with one peer."""
        return self._send(peer_id, "sync_request", self._payload())

    # ------------------------------------------------------------------
    # incoming messages
    # ------------------------------------------------------------------
    def receive(self, message):
        """Handle one inbound message."""
        self.stats["received"] += 1
        if message.sender not in self.members:
            self.stats["rejected"] += 1
            return False
        payload = message.payload or {}
        self.merge_version(payload.get("version", {}))
        self.merge_members(payload.get("members", []))
        for rumor_id, rumor in (payload.get("rumors") or {}).items():
            self._accept_rumor(rumor_id, rumor, message.sender)
        if message.kind == "sync_request":
            self._answer_sync_request(message, payload)
        return True

    def _answer_sync_request(self, message, payload):
        digest = payload.get("version", {})
        unknown = self._rumors_unknown_to(digest)
        self._send(message.sender, "sync_reply", self._payload(unknown))

    # ------------------------------------------------------------------
    # state merging
    # ------------------------------------------------------------------
    def merge_version(self, incoming):
        """Fold a foreign version vector into this node's own view."""
        for origin, counter in (incoming or {}).items():
            known = self.version.get(origin, 0)
            self.version[origin] = min(known, int(counter))
        return dict(self.version)

    def merge_members(self, entries):
        """Fold foreign member entries into the local table."""
        changed = 0
        for entry in entries or []:
            if self._merge_member_entry(entry):
                changed += 1
        return changed

    def _merge_member_entry(self, entry):
        if not isinstance(entry, dict):
            return False
        member_id = entry.get("member_id")
        if not member_id:
            return False
        heartbeat = int(entry.get("heartbeat", 0))
        join_epoch = int(entry.get("join_epoch", 0))
        tombstone = self.tombstones.get(member_id)
        if tombstone is not None and join_epoch < tombstone:
            return False
        current = self.members.get(member_id)
        if current is None:
            self.members[member_id] = MemberState(member_id, heartbeat=heartbeat,
                                                  join_epoch=join_epoch,
                                                  status=ALIVE, last_seen=self.clock.now())
            return True
        if join_epoch < current.join_epoch:
            return False
        if join_epoch > current.join_epoch:
            current.join_epoch = join_epoch
            self.mark_alive(member_id, heartbeat)
            return True
        if not heartbeat_newer(heartbeat, current.heartbeat):
            return False
        self.mark_alive(member_id, heartbeat)
        return True

    def mark_alive(self, member_id, heartbeat=None):
        """Mark a member as reachable again."""
        member = self.members.get(member_id)
        if member is None:
            return False
        if heartbeat is not None:
            member.heartbeat = heartbeat
        member.status = ALIVE
        member.last_seen = self.clock.now()
        return True

    def detect_failures(self):
        """Advance the local failure detector by one step."""
        now = self.clock.now()
        for member in self.members.values():
            if member.member_id == self.node_id or member.status == DEAD:
                continue
            if member.suspect_deadline is not None:
                if now >= member.suspect_deadline:
                    member.status = DEAD
                    member.suspect_deadline = None
                continue
            if now - member.last_seen > self.suspicion_timeout:
                member.status = SUSPECT
                member.suspect_deadline = now + self.suspicion_timeout
        return {m.member_id: m.status for m in self.members.values()}

    # ------------------------------------------------------------------
    # rumour handling
    # ------------------------------------------------------------------
    def _dedup_key(self, rumor_id, from_peer):
        """Return the bookkeeping key for a rumour that arrived from a peer."""
        return (from_peer, rumor_id)

    def _accept_rumor(self, rumor_id, payload, from_peer):
        """Store a rumour once and schedule it for relay."""
        if not isinstance(payload, dict):
            return False
        key = self._dedup_key(rumor_id, from_peer)
        if key in self.seen:
            self.stats["duplicates"] += 1
            return False
        rumor = {
            "origin": payload.get("origin", from_peer),
            "counter": int(payload.get("counter", 0)),
            "key": payload.get("key"),
            "value": payload.get("value"),
        }
        self.seen.add(key)
        self.rumors[rumor_id] = rumor
        self.relayed[key] = {"rumor_id": rumor_id, "peers": set()}
        self.merge_version({rumor["origin"]: rumor["counter"]})
        self.stats["accepted"] += 1
        self._push_now(key)
        return True

    def _rumors_unknown_to(self, digest):
        """Rumours this node holds that the digest does not account for."""
        unknown = {}
        for rumor_id, rumor in self.rumors.items():
            origin = rumor.get("origin")
            counter = int(rumor.get("counter", 0))
            if int((digest or {}).get(origin, 0)) > counter:
                unknown[rumor_id] = dict(rumor)
        return unknown

    # ------------------------------------------------------------------
    # relaying
    # ------------------------------------------------------------------
    def tick(self):
        """One protocol round."""
        self._relay_round()
        self.detect_failures()
        return self.clock.now()

    def pending_relays(self):
        """Number of (rumour, peer) pairs that still need to be pushed."""
        reachable = set(self.peer_ids())
        total = 0
        for entry in self.relayed.values():
            total += len(reachable - entry["peers"])
        return total

    def _relay_round(self):
        """Push pending rumours to a bounded fan-out of peers."""
        for key in sorted(self.relayed, key=str):
            entry = self.relayed[key]
            if self.rumors.get(entry["rumor_id"]) is None:
                continue
            for target in self._fanout_targets():
                if target in entry["peers"]:
                    continue
                self._relay_rumor(target, entry)

    def _push_now(self, key):
        """Push a freshly accepted rumour immediately to the current fan-out."""
        entry = self.relayed.get(key)
        if entry is None:
            return 0
        payload = self.rumors.get(entry["rumor_id"])
        if payload is None:
            return 0
        pushed = 0
        for target in self._fanout_targets():
            self._send(target, "rumor", self._payload({entry["rumor_id"]: payload}))
            pushed += 1
        return pushed

    def _relay_rumor(self, target, entry):
        payload = self.rumors.get(entry["rumor_id"])
        if payload is None:
            return False
        self._send(target, "rumor", self._payload({entry["rumor_id"]: payload}))
        entry["peers"].add(target)
        return True

    def _fanout_targets(self):
        """Pick up to fanout reachable peers for the next push."""
        candidates = [m.member_id for m in self.members.values()
                      if m.status != DEAD]
        if not candidates:
            return []
        return self.random_source.sample(candidates, min(self.fanout, len(candidates)))

    def peer_ids(self, statuses=(ALIVE, SUSPECT)):
        """Ids of the known members that are still considered reachable."""
        return [m.member_id for m in self.members.values()
                if m.member_id != self.node_id and m.status in statuses]

    def rumor_ids(self):
        """Ids of the rumours this node stores."""
        return tuple(sorted(self.rumors))

    # ------------------------------------------------------------------
    # message plumbing
    # ------------------------------------------------------------------
    def _payload(self, rumors=None):
        payload = {
            "version": dict(self.version),
            "members": [m.as_dict() for m in self.members.values()],
        }
        if rumors:
            payload["rumors"] = rumors
        return payload

    def _send(self, recipient, kind, payload):
        self.message_counter += 1
        message = Message("%s-%d" % (self.node_id, self.message_counter),
                          self.node_id, recipient, kind, payload)
        self.network.send(message)
        self.stats["sent"] += 1
        return message


class Simulation:
    """Deterministic driver wiring clock, network and nodes together."""

    def __init__(self, node_ids, seed=0, fanout=DEFAULT_FANOUT,
                 suspicion_timeout=DEFAULT_SUSPICION_TIMEOUT, latency=1, clock=None):
        self.clock = clock if clock is not None else SimClock()
        self.network = MessageNetwork(self.clock, latency=latency)
        self.nodes = {}
        for index, node_id in enumerate(node_ids):
            self.nodes[node_id] = GossipNode(node_id, self.network, self.clock,
                                             SeededRandom(seed + index),
                                             fanout=fanout,
                                             suspicion_timeout=suspicion_timeout,
                                             join_epoch=index + 1)
        self._introduce()

    def _introduce(self):
        """Give every node the member table of the whole cluster."""
        now = self.clock.now()
        for node in self.nodes.values():
            for member_id, peer in self.nodes.items():
                if member_id == node.node_id:
                    continue
                node.members[member_id] = MemberState(member_id,
                                                      heartbeat=peer.heartbeat,
                                                      join_epoch=peer.join_epoch,
                                                      last_seen=now)

    def deliver(self):
        return self.network.deliver_due(self.nodes)

    def step(self, rounds=1):
        """Advance the clock, deliver due messages and run one round per node."""
        for _ in range(rounds):
            self.clock.advance(1)
            self.deliver()
            for node in self.nodes.values():
                node.tick()
        return self.clock.now()

    def sent_total(self):
        return sum(node.stats["sent"] for node in self.nodes.values())

    def pending_relays(self):
        return sum(node.pending_relays() for node in self.nodes.values())

    def run_until_quiet(self, max_rounds=40, idle_rounds=2):
        """Run rounds until no message is in flight and nothing is pending."""
        quiet = 0
        rounds = 0
        while rounds < max_rounds:
            self.step()
            rounds += 1
            if self.network.pending() == 0 and self.pending_relays() == 0:
                quiet += 1
                if quiet >= idle_rounds:
                    break
            else:
                quiet = 0
        return rounds
