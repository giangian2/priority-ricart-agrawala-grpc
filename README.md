![Logo del progetto](./res/smartfab.svg)

# SMARTFAB

The system consists of a peer-to-peer network of **production lines** with an
**administration server** used for line-bootstrap and telemetry. Coordination
for access to calibration mode is decentralized and happens exclusively through
gRPC between peers.

### Production line

This is the edge node, organized as a concurrent pipeline of *threads* that
communicate through shared *buffers* following a producer–consumer scheme:

- **MonitoringSensor** — thread that simulates the sensor and deposits vibration
  measurements into the `MeasurementBuffer`.
- **SlidingWindowProcessor** — thread that consumes the measurements, computes
  the average over a sliding window and writes the result into the
  `AveragesBuffer`. When the average exceeds the critical threshold, emits
  `CriticalStatusEvent`.
- **AveragesConsumer** — thread that pulls the aggregated averages
  them over **MQTT** (telemetry).
- **MeasurementBuffer / AveragesBuffer** — thread-safe buffers.
- **gRPC server** (`CalibrationServiceImpl`) — exposes `requestCalibration`,
  `grantCalibrationAccess` and `joinP2P`: it is the entry point for the mutual
  exclusion messages and for new peers joining the network.
- **Mutual exclusion engine** (`RicartEngine`) — sole owner of the algorithm
  state and of the line's state machine (*Idle*, *Waiting*, *Calibrating*), and
  the only holder of a lock; see [Concurrency and locking](#concurrency-and-locking).

Everything is wired together by a small **event bus** (`EventDispatcher` /
`EventListener`): the `ProductionLine` observes the domain events
(`CriticalStatusEvent`, `CalibrationGrantEvent`, `CalibrationTerminatedEvent`)
and reacts by suspending the sensor, requesting calibration or releasing calibration.

### Administration REST client

`PeerRestClient` is used at startup: the line registers with the a
server and receives in return the list of already-active peers, with which it
opens the gRPC channels. Once the bootstrap is complete, the line does not
depends on the server to coordinate.

### Spring administration server

REST server (Spring) responsible for peer **registration and discovery** and for
**telemetry collection**. It does not take part in mutual exclusion
coordination, only a network registry and a data collector.

### MQTT

Outbound telemetry channel: each line publishes its aggregated averages to an
MQTT broker, keeping data dissemination separate from the coordination process
(which stays entirely on point-to-point gRPC).

## Algorithm: Ricart–Agrawala with criticality-based priority

Mutual exclusion over access to calibration adopts the **Ricart–Agrawala**
algorithm, but replaces Lamport's logical clock with an application-defined
priority rule. The total order among concurrent requests is the pair
`(criticality, id)`: the higher criticality wins and, on a tie, the
identifier. Criticality is computed as `(v̄ − θ) / θ`, fixed upon entering the
waiting state and kept constant until calibration ends.

The base protocol remains the classic one: the line broadcasts its ip,port and id to
all peers, collects the grants and enters the critical section only after every
peer has agreed. On exit it sends out the deferred grants.

### The problem

Standard Ricart–Agrawala orders requests with a Lamport timestamp, which is
*causal*: if a line has seen your request, any request it issues afterwards
carries a larger timestamp, so it is automatically placed *after* you.

Criticality does not have this property. A line can grant permission while it is
still in *Running* it isn't competing yet, so it says "OK" to everyone and
only later cross the threshold and ask for calibration with a *higher*
criticality. At that point it has already handed out a permission that no longer
reflects reality: it competes and wins, while the line it granted still holds
that stale "OK". Both can then reach a full set of permissions and enter
calibration together, breaking mutual exclusion.

### The fix — restart

![Why the restart is needed](res/restart.svg)

When a waiting line receives a request from a *more critical* one, it yields:
it grants the permission, then **restarts** its own attempt it throws away the
permissions collected so far and broadcasts its request again. Because the stale
grant is discarded, the line can no longer enter with it; it will regain access
only after the more critical line releases the resource. This keeps a single
line in calibration even when several cross the threshold at nearly the same
time.

### The fix — round

![Why the round is needed](res/round.svg)

The restart alone is not enough on an asynchronous network, where a message can
be delivered late. A permission granted "blindly" may arrive *after* the request
that should have invalidated it and it would be counted as valid again. To
prevent this, every request carries a local **attempt number (round)**, which
each grant echoes back. A line accepts a grant only if its round matches the
current attempt; a grant carrying an old round is simply dropped. 

## Concurrency and locking

A line runs several threads that all reach the same algorithm state: the sensor
pipeline asks for calibration, the gRPC server threads deliver requests and
grants coming from other peers, and the shutdown hook waits for the critical
section to be released. `RicartEngine` is the single owner of that state and the
only holder of a lock.

The rule is **decide under the lock, talk to the network outside it**. Every
entry point funnels through `step()`, which runs the decision inside the monitor
and collects its side effects — the messages to send, the events to publish —
into an `Outbox` and a pending-events list. Both are drained and executed only
after the monitor has been released. The state machine never sends anything: it
appends, and someone else flushes.

### Why the collaborators are not thread-safe

`PeerRegistry`, `RoundState`, `DeferredGrants` and `Outbox` carry no
`synchronized` of their own, and that is the design, not an omission. They are
not shared objects: they are owned by the engine and reached only with its
monitor already held — either from inside `step()`, or from the `synchronized`
block in `shutdown()`. The state classes touch them only through `RicartContext`,
which is implemented by the engine and never called from anywhere else.

An earlier version did lock each of them individually, and it was strictly
worse. `GrantTracker.hasQuorum(requiredGrants)` was `synchronized`, but
`requiredGrants` came from the peer list under a *different* lock: each step was
atomic and the decision as a whole was not, so a peer could join or leave in the
gap between counting the members and comparing that count with the grants
collected. The unit of atomicity that actually matters is the whole decision —
*record this grant, count the members, enter the section, queue the outgoing
messages* — and today that unit is exactly the body of `step()`. Re-adding inner
locks would be reentrant and harmless at runtime, but it would advertise
something false: that these classes can be used without the engine's monitor.

### The two exceptions

`PeerTransport.connect()` and `disconnect()` are called from *inside* the
monitor, in `onJoinPeerReceived` and `forgetPeer`. That is deliberate: a channel
has to appear and disappear atomically with the membership change that causes
it, otherwise a message decided in the same step could find nothing to travel
on. Two properties make it safe, and both are worth stating because neither is
permanent:

1. **The lock order is one-way.** The engine takes its own monitor first and the
   transport's `channelsLock` second, and nothing under `channelsLock` ever
   calls back into the engine — the gRPC callbacks in `GrpcPeerTransport` only
   log. It is a leaf lock, so the two cannot form a cycle and no deadlock is
   reachable.
2. **Neither call touches the network.** A gRPC `ManagedChannel` is lazy:
   `build()` allocates an object and dials nothing, the TCP connection is opened
   by the first RPC. And `ManagedChannel.shutdown()` starts a graceful shutdown
   and returns immediately, leaving in-flight RPCs to finish; `awaitTermination()`,
   the blocking one, is never called there. In practice the two calls are a map
   insertion and a map removal.

### What changes with persistent streams

The migration described in `GRPC_STREAMING_DESIGN.md` invalidates property 2.
Once every peer is reached through one long-lived `communicate()` stream,
`connect()` has to *open* that stream — an actual RPC — and `disconnect()` has to
write `onCompleted()` on it. Both become network I/O performed while holding the
algorithm lock, and a write on an HTTP/2 stream blocks as soon as the peer's
flow-control window is exhausted.

The failure that follows is a distributed deadlock, and it is silent. The thread
holding the monitor blocks writing to a slow peer; every inbound gRPC handler
queues behind that monitor; the line therefore stops consuming its own inbound
messages; its receive window fills; and the peer writing to it blocks in turn.
Two live processes, no exception thrown, nothing in any log to explain it. This
is the exact scenario `Outbox` was introduced to make unreachable.

So the migration carries a prerequisite: the connection lifecycle has to go
through the same deferred flush as the messages. Either it is queued as an
ordered action in the `Outbox` — ordered, so that a grant decided before a peer
is dropped still goes out before its channel is closed — or it disappears
altogether, by having the transport open channels on demand and keeping
`PeerRegistry` as the only place where membership is decided.

## Usage

The `Makefile` wraps build and run of every component. The `run-*` targets each
depend on `build` and on `run-mqtt-broker`, so they recompile the sources and
bring up the MQTT broker automatically before starting.

Prerequisites: a JDK and Docker (the broker runs via `docker compose`); the
Gradle wrapper is bundled, no local Gradle needed.

| Command | Purpose |
| --- | --- |
| `make build` | Compile the sources (`./gradlew compileJava`). |
| `make clean` | Remove build artifacts. |
| `make run-mqtt-broker` | Start the MQTT broker (`docker compose up -d`). |
| `make run-admin-server` | Start the Spring administration server (registration, discovery, telemetry). |
| `make run-admin-client` | Start the administration CLI. |
| `make run-pl-instance ARGS="<id> <ip> <port> <admin server url>"` | Start a production line with the given identifier and gRPC endpoint. |



                                                                                           


