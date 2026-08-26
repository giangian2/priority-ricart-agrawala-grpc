# Refactoring architetturale — algoritmo, transport e bootstrap

> **Scopo**: separare le responsabilità oggi concentrate in `RicartMutualExclusionPeer`
> e `GrpcPeer`, rendere il bootstrap della rete P2P deterministico, e preparare il
> terreno alla migrazione a gRPC streaming descritta in `GRPC_STREAMING_DESIGN.md`.
>
> **Vincoli**: Java 17, nessuna dipendenza nuova. Le primitive di sincronizzazione
> di supporto (es. il latch di attesa della join) sono implementate a mano con
> `wait`/`notify`, non prese da `java.util.concurrent`.

## Indice

1. [I problemi](#1-i-problemi)
2. [Principi della soluzione](#2-principi-della-soluzione)
3. [La struttura finale](#3-la-struttura-finale)
4. [Step 1 — I due bug del bootstrap](#step-1--i-due-bug-del-bootstrap)
5. [Step 2 — Messaggi di dominio e port del transport](#step-2--messaggi-di-dominio-e-port-del-transport)
6. [Step 3 — Outbox e `step()`: il single point](#step-3--outbox-e-step-il-single-point)
7. [Step 4 — Le due macro-aree: IN e OUT](#step-4--le-due-macro-aree-in-e-out)
8. [Step 5 — Join con handshake](#step-5--join-con-handshake)
9. [Step 5-bis — Il bug del `Context` gRPC](#step-5-bis--il-bug-del-context-grpc-trovato-durante-il-test)
10. [Step 6 — Migrazione a gRPC streaming](#step-6--migrazione-a-grpc-streaming)
11. [Tabella prima/dopo](#10-tabella-primadopo)

---

## 1. I problemi

### 1.1 `RicartMutualExclusionPeer` ha sei responsabilità

| # | Responsabilità | Evidenza nel codice |
|---|---|---|
| 1 | Comandi applicativi | `requestCalibration`, `releaseCalibration`, `shutdown` |
| 2 | Handler di rete | `onRequestReceived`, `onGrantReceived`, `onJoinPeerReceived`, `onExitPeerReceived` |
| 3 | Contesto della macchina a stati | `implements RicartContext` (9 metodi) |
| 4 | Dispatch di eventi | `extends EventDispatcher<ProductionLineEvent>` |
| 5 | Costruzione di messaggi protobuf | `CalibrationRequest.newBuilder()` in **4 punti** |
| 6 | Pubblicazione MQTT | `MqttClientManager.getInstance()` dentro `setState` |

La #6 è una dipendenza da singleton **nascosta nel costruttore di nessuno**: non
compare in nessuna firma, quindi la classe non è istanziabile in un test senza
un broker MQTT raggiungibile.

### 1.2 Il protobuf attraversa l'astrazione che dovrebbe isolarlo

Il javadoc di `RicartMutualExclusionPeer` afferma:

> *"This class no longer extends the transport (GrpcPeer), it OWNS a Peer, so the
> algorithm can be tested with an in-memory transport."*

Ma `Peer` dichiara `sendRequest(CalibrationRequest, int)`, e `CalibrationRequest`
è una classe **generata da protobuf**. Qualunque transport finto deve quindi
dipendere da protobuf: l'astrazione non isola nulla.

### 1.3 `Peer` mescola membership e messaggistica, e perde incapsulamento

`addPeer` / `removePeer` / `getAllPeers` (membership) convivono con
`sendRequest` / `sendGrant` / `sendRequestToAll` (messaggistica). Peggio:
`getAllPeersStub()` restituisce `List<PeerStub>`, cioè espone all'algoritmo il
`ManagedChannel` gRPC. È la falla di incapsulamento più grave del progetto.

### 1.4 Si fa I/O di rete tenendo il lock

`grantTo` spedisce sulla rete ed è invocato dagli stati dentro metodi
`synchronized`. Con le RPC unarie fire-and-forget non blocca mai, quindi oggi
non esplode. Con lo streaming (`GRPC_STREAMING_DESIGN.md`) `onNext` **può
bloccare** sul flow-control HTTP/2 mentre il thread di ricezione gRPC attende
lo stesso monitor: **deadlock**.

### 1.5 Locking ridondante e ambiguo

`GrantTracker` e `DeferredGrants` sono `synchronized` al loro interno, ma sono
sempre invocati con il monitor dell'algoritmo già preso. In più
`RicartMutualExclusionPeer` eredita `EventDispatcher.notify(T)` e usa
`Object.notifyAll()`/`wait()` sullo stesso oggetto: due `notify` con semantica
completamente diversa nella stessa classe.

### 1.6 Il bootstrap ha due bug di ordinamento

Descritti in dettaglio nello [Step 1](#step-1--i-due-bug-del-bootstrap).

---

## 2. Principi della soluzione

1. **`smartfab.algorithms.ricart` non deve importare né `io.grpc` né
   `smartfab.Smartfab`.** È una proprietà verificabile con un `grep`, non
   un'intenzione.
2. **Chi decide non spedisce.** La macchina a stati accumula messaggi in una
   `Outbox`; l'engine li drena **fuori** dal lock.
3. **Un solo punto di ingresso e di uscita**: il metodo `step()` di
   `RicartEngine`. Nessun metodo pubblico tocca il transport direttamente.
4. **La membership è separata dai canali**: `PeerRegistry` (chi conta per il
   quorum) vs. transport (chi ha un canale aperto).
5. **Ogni step compila e gira.** Gli step 2–4 sono a comportamento invariato.

---

## 3. La struttura finale

```
                  ProductionLine (applicazione)
                    │ comanda              ▲ eventi
                    ▼                      │
       ╔══════════════════════════════════════════════╗
       ║              RicartEngine                    ║  ◄── SINGLE POINT
       ║   unico proprietario dello stato, unico lock ║
       ║                                              ║
       ║   implements MutualExclusionAlgorithm  (OUT) ║
       ║              PeerEventHandler          (IN)  ║
       ║              RicartContext         (stati)   ║
       ╚══════════════════════════════════════════════╝
          │              │              │           │
    ┌─────▼──────┐ ┌─────▼─────┐  ┌─────▼────┐  ┌──▼──────┐
    │PeerRegistry│ │RoundState │  │Deferred  │  │ Outbox  │
    │ membership │ │round+grant│  │Grants    │  │messaggi │
    └────────────┘ └───────────┘  └──────────┘  └────┬────┘
                                                     │ drenata FUORI dal lock
                                                ┌────▼──────────┐
                                                │ PeerTransport │ ◄── zero protobuf
                                                └───────────────┘
                                                        ▲
                                          GrpcPeerTransport + MessageCodec
                                          (il protobuf vive SOLO qui)
```

### Package

| Package | Contenuto | Regola |
|---|---|---|
| `smartfab.algorithms.ricart` | Algoritmo puro + port (`PeerTransport`, `PeerMessage`, `PeerEventHandler`) | **zero** import di `io.grpc` / `smartfab.Smartfab` |
| `smartfab.model.edge` | Adapter gRPC (`GrpcPeerTransport`, `MessageCodec`, `PeerChannel`, `CalibrationServiceImpl`) e MQTT (`MqttStateListener`) | è l'unico posto dove vive il protobuf |

### File

**Nuovi in `algorithms.ricart`**: `PeerMessage`, `PeerTransport`,
`PeerEventHandler`, `PeerRegistry`, `Outbox`, `RoundState`, `JoinOutcome`,
`Latch`, `RicartEngine`.

**Nuovi in `model.edge`**: `GrpcPeerTransport`, `MessageCodec`, `PeerChannel`,
`MqttStateListener`.

**Nuovi in `model.events`**: `PeerStateChangedEvent`.
`EventDispatcher` riscritto per consegnare in ordine (vedi 4.2).

**Eliminati**: `Peer`, `GrpcPeer`, `PeerStub`, `RequestContext`, `GrantTracker`,
`RicartMutualExclusionPeer`, `ObserverFactory`.

---

## Step 1 — I due bug del bootstrap

*Indipendenti dal resto. Da fare per primi.*

### Bug 1: la registrazione al registry non è atomica

`ProductionLineController.registerProductionLine`:

```java
Map<PeerInfo, String> updatedPeers = peerService.getAll();   // ① leggi
peerService.registerPeer(peer);                              // ② scrivi
return ResponseEntity.ok(new ArrayList<>(updatedPeers.keySet()));
```

Due chiamate `synchronized` **distinte** sul repository. Se le linee A e B
registrano insieme, entrambe possono eseguire ① prima che l'altra esegua ②, e
**ricevono una lista che esclude l'altra**. A quel punto `otherPeerCount() == 0`
per entrambe, e `requestCalibration` entra **subito** in `CalibratingState`
credendo di essere sola: **due linee calibrano contemporaneamente, la mutua
esclusione è violata.**

Il commento in `ProductionLine.main` afferma proprio l'invariante che il codice
non fornisce:

> *"since register and remove are synchronized on the server, it can't happen
> that the new registering line will receive the exited peer address and port."*

Sono `synchronized` singolarmente; la **sequenza** read-then-write no.

**Fix** — una sola operazione atomica sotto il monitor del repository:

```java
// GlobalLockRepository
public synchronized List<K> saveAndSnapshot(K key, V value) {
    var before = List.copyOf(storage.keySet());   // lo stato PRIMA di me
    storage.put(key, value);
    return before;
}
```

Nello stesso file, `findAll()` restituisce `this.storage` per riferimento:
aliasing su una mappa condivisa. Va restituita una copia.

### Bug 2: il server gRPC parte dopo la registrazione

Ordine attuale in `main`: `ProductionLine.init()` (che fa `registerPeer` +
`addPeer`) → **poi** `grpcServer.start()` → poi `joinPeerNetwork()`.

Esiste quindi una finestra in cui la linea è **registrata e visibile a tutti, ma
la sua porta gRPC non è in ascolto**. Chi registra in quella finestra riceve il
suo indirizzo e apre un canale → *connection refused*. Oggi non si nota perché
le RPC unarie sono fire-and-forget con `emptyStreamObserver()`; con lo streaming
diventerebbe un `onError` immediato.

Ironicamente il commento sopra la costruzione delle dipendenze dice già
`WE NEED TO START THE GRPC SERVER BEFORE THE PRODUCTION LINE` — è l'intenzione
giusta, non applicata.

**Fix**: costruire algoritmo e transport, avviare il server gRPC, **poi**
registrarsi al registry.

---

## Step 2 — Messaggi di dominio e port del transport

*Comportamento invariato.*

### 2.1 `PeerMessage`

```java
public sealed interface PeerMessage {
    record Request (int senderId, double criticality, int round) implements PeerMessage {}
    record Grant   (int senderId, int round)                     implements PeerMessage {}
    record Leave   (int senderId)                                implements PeerMessage {}
}
```

Tre record. La **join non è un `PeerMessage`**: non è fire-and-forget, ha un
valore di ritorno (l'ACK), quindi è un'operazione a sé del transport (Step 5).

Cosa sparisce con questa singola mossa:

- le 4 `CalibrationRequest.newBuilder()` sparse nell'algoritmo;
- il marcatore magico `priority = 0` che oggi significa "questo è un grant";
- l'ambiguità `timestamp` `double` nel proto ma `int round` nel codice;
- la dipendenza da protobuf di qualunque transport di test.

> **Nota Java 17**: `sealed` limita la gerarchia, ma lo `switch` per pattern su
> oggetti è standard solo da Java 21. Il `MessageCodec` usa quindi una catena di
> `if (msg instanceof X x)` (pattern matching per `instanceof`, standard da 16)
> chiusa da un `throw`. Passando a Java 21 diventerebbe uno `switch` esaustivo
> verificato dal compilatore.

### 2.2 `PeerTransport` sostituisce `Peer`

```java
public interface PeerTransport {
    JoinOutcome joinNetwork(PeerInfo me, List<PeerInfo> candidates, long timeoutMillis);
    void connect(PeerInfo peer);
    void disconnect(int peerId);
    void send(int peerId, PeerMessage message);
    void sendAll(Collection<Integer> peerIds, PeerMessage message);
    void shutdown();
}
```

`getAllPeers()` e `getAllPeersStub()` **spariscono**: la membership appartiene a
`PeerRegistry`, il transport tiene i canali e basta — privati.
`sendReleaseToAll` sparisce: è codice morto (Ricart-Agrawala non ha un messaggio
di release, il rilascio è un grant implicito — lo dice il javadoc stesso di
`MutualExclusionAlgorithm`).

### 2.3 `MessageCodec`

Unico punto di conversione dominio ↔ protobuf, in `model.edge`. Oggi mappa su
tre RPC unarie diverse; dopo lo Step 6 mapperà su un unico `AlgorithmMessage`
con `oneof`. **È l'unico file che cambia in entrambi i casi.**

---

## Step 3 — Outbox e `step()`: il single point

*Comportamento invariato, ma sparisce il rischio di deadlock.*

```java
final class Outbox {
    record Envelope(int targetId, PeerMessage message) {}
    private final List<Envelope> pending = new ArrayList<>();

    void to(int peerId, PeerMessage m) { pending.add(new Envelope(peerId, m)); }
    void toAll(Collection<Integer> ids, PeerMessage m) { ids.forEach(id -> to(id, m)); }
    List<Envelope> drain() { var copy = List.copyOf(pending); pending.clear(); return copy; }
}
```

E il cuore dell'engine:

```java
/**
 * SINGLE POINT: ogni ingresso e ogni uscita passano di qui.
 *   decide sotto lock (zero I/O)  →  spedisce fuori dal lock (zero lock)
 */
private void step(Runnable decision) {
    List<Outbox.Envelope>     toSend;
    List<ProductionLineEvent> toPublish;

    synchronized (this) {
        decision.run();                  // muta lo stato, riempie outbox ed eventi
        toSend    = outbox.drain();
        toPublish = pendingEvents.drain();
    }

    toSend.forEach(e -> transport.send(e.targetId(), e.message()));
    toPublish.forEach(dispatcher::notify);
}
```

Ogni metodo pubblico diventa una riga:

```java
@Override public void onRequestReceived(int senderId, double criticality, int round) {
    step(() -> state.onRequest(this, senderId, criticality, round));
}
```

La proprietà che interessa è verificabile a colpo d'occhio: **nessun metodo
pubblico tocca `transport` direttamente**. Il deadlock I/O-sotto-lock diventa
strutturalmente impossibile, non evitato per disciplina.

### 3.1 `RicartContext` dimagrisce

```java
interface RicartContext {
    int    peerId();
    double myCriticality();
    int    myRound();

    void    grantTo(int peerId, int round);     // → OUTBOX, non spedisce
    void    deferGrant(int peerId, int round);
    void    recordGrant(int fromPeerId);
    boolean hasFullQuorum();

    void enterCriticalSection(int triggeringPeerId);
    void yieldAndRetry();                       // ex restart()
}
```

Due cambiamenti sostanziali:

**`grantTo` accoda invece di spedire.** Conseguenza: la macchina a stati diventa
**pura** — nessun I/O, nessun lock, nessuna rete — e si testa con un
`RicartContext` finto in cinque righe, senza gRPC e senza thread. È il guadagno
di riusabilità che il javadoc attuale promette ma che oggi non esiste.

**`restart()` → `yieldAndRetry()`.** Oggi `restart()` rientra in
`requestCalibration(currentCriticality())`, cioè riusa un metodo **pubblico**
dell'API applicativa per uno scopo interno, con effetti collaterali non ovvi
(incrementa il round, azzera i grant, ripubblica lo stato MQTT, rientra nel
monitor già preso). Il rename non basta: il punto è rendere **esplicita** la
scelta di aprire un round nuovo invece di riaprire la raccolta sullo stesso
round. È il punto in cui questa implementazione si scosta da Ricart-Agrawala
canonico — che usa timestamp di Lamport e non ha bisogno di ritentare — e merita
di essere visibile.

### 3.2 Fusione delle micro-classi

`RequestContext` + `GrantTracker` → `RoundState`: stessa vita, stesso
proprietario, stesso lock. Sparisce il `synchronized` ridondante, perché
l'engine è già l'unico titolare.

`DeferredGrants` **resta separata**: i differiti sopravvivono al round corrente e
portano il round *del richiedente*, non il proprio. Ciclo di vita diverso,
classe diversa.

---

## Step 4 — Le due macro-aree: IN e OUT

*Comportamento invariato.*

```java
/** ═══ IN — tutto ciò che ARRIVA dalla rete ═══ */
public interface PeerEventHandler {
    void onRequestReceived(int senderId, double criticality, int round);
    void onGrantReceived  (int senderId, int round);
    void onJoinPeerReceived(int senderId, String senderAddress, int senderPort);
    void onExitPeerReceived(int senderId);
    void onPeerUnreachable (int senderId);       // crash detection (Step 6)
}

/** ═══ OUT — tutto ciò che l'APPLICAZIONE chiede all'algoritmo ═══ */
public interface MutualExclusionAlgorithm {
    boolean join(List<PeerInfo> fromRegistry, long timeoutMillis);
    void    requestCalibration(double criticality);
    void    releaseCalibration();
    void    shutdown();
    void    subscribe(EventListener<ProductionLineEvent> listener);
}
```

`CalibrationServiceImpl` dipende **solo** da `PeerEventHandler`;
`ProductionLine` **solo** da `MutualExclusionAlgorithm`. Oggi invece
`CalibrationServiceImpl` riceve l'intera `MutualExclusionAlgorithm` e potrebbe
chiamare `requestCalibration` dal thread di rete.

`subscribe` torna sull'interfaccia dei comandi: elimina il cast non controllato
in `main`

```java
@SuppressWarnings("unchecked")
var disp = (EventDispatcher<ProductionLineEvent>) pl.algorithm;
```

e permette di passare da ereditarietà (`extends EventDispatcher`) a
**composizione**, il che risolve anche la convivenza ambigua fra
`EventDispatcher.notify(T)` e `Object.notifyAll()` nella stessa classe.

### 4.1 Un solo pattern event-listener, e ordinato

Le transizioni di stato **non** hanno un'interfaccia di ascolto propria:
sarebbe stato un secondo pattern event-listener identico nella forma a quello
gia' presente. Viaggiano sullo stesso dispatcher di tutti gli altri eventi:

```java
public class PeerStateChangedEvent extends ProductionLineEvent {
    private final int    peerId;
    private final String stateName;
}
```

`MqttStateListener` diventa un `EventListener<ProductionLineEvent>` come
`ProductionLine`, e ci si iscrive allo stesso modo:

```java
algorithm.subscribe(pl);
algorithm.subscribe(new MqttStateListener());
```

Questo tiene comunque il singleton MQTT fuori dall'engine: con la lookup inline
in `setState` l'algoritmo non era istanziabile senza un broker raggiungibile, e
nessuna firma lo diceva.

**Composizione, non ereditarieta'.** `RicartEngine` compone
`EventDispatcher` esattamente come fa `SlidingWindowProcessor:31`; nessuna
classe del progetto lo estende. `RicartMutualExclusionPeer extends
EventDispatcher` era l'eccezione, ed era la ragione del cast non controllato in
`main`. `SlidingWindowProcessor` non potrebbe comunque ereditarlo, perche'
estende gia' `Thread`.

### 4.2 `EventDispatcher`: consegna ordinata

Unificare i due pattern non era possibile prima di correggere il dispatcher.
`notify(T)` faceva:

```java
for (EventListener<T> listener : listeners) {
    new Thread(() -> listener.onEvent(event)).start();   // un thread PER EVENTO
}
```

Fire-and-forget, ma **senza alcuna garanzia d'ordine**: le tre transizioni
`IDLE`, `WAITING`, `CALIBRATING` partono su tre thread indipendenti e possono
raggiungere il listener in qualsiasi ordine, lasciando il cruscotto su uno stato
in cui la linea non e'. Lo stesso difetto valeva gia' per gli eventi di
calibrazione.

Sostituito da **una sola coda drenata da un unico thread di dispatch**
(daemon, avviato pigramente alla prima pubblicazione):

- l'ordine di pubblicazione e' l'ordine di consegna;
- un thread invece di N per evento;
- resta asincrono, che e' obbligatorio: un listener puo' bloccare per secondi
  (la calibrazione e' simulata con uno `sleep` dentro `onEvent`) e chi pubblica
  e' spesso un thread del server gRPC;
- i listener vengono invocati **fuori** dal monitor del dispatcher.

Rinominato `notify(T)` in `publish(T)`: la classe e' pensata per essere composta
dentro oggetti che usano anche il monitor intrinseco, e avere `notify(T)`
accanto a `Object.notify()` e' una trappola.

---

## Step 5 — Join con handshake

*Primo cambio di comportamento reale.*

### 5.1 Il problema

Oggi `ProductionLine.init` fa `addPeer` su **ogni** peer restituito dal registry,
prima di aver scambiato un solo byte con lui. Se uno di quelli è già morto,
entra comunque nella topologia, `otherPeerCount()` lo conta, e il quorum non è
più raggiungibile: `WaitingState` resta bloccato per sempre.

### 5.2 La soluzione: l'ACK ce l'abbiamo già

`joinP2P` è una RPC **unaria** che ritorna `Empty`. La sua chiusura è già un
acknowledgement: si tratta solo di usarlo.

```
apri canale → invia joinP2P → attendi onCompleted() → SOLO ORA registra il peer
```

`addPeer` va quindi spezzato in due operazioni oggi fuse:

```
oggi:   addPeer(p)  =  [apri canale]  +  [registra nella topologia]
dopo:   connect(p)  =  [apri canale]                                  ← non conta ancora
        commit(p)   =                     [registra nella topologia]  ← solo su ACK
```

### 5.3 Perché `onCompleted()` e non `onNext()`

`onNext(Empty)` significa solo "il server ha scritto il payload".
`onCompleted()` scatta sul trailer con `grpc-status: 0`, cioè **l'handler remoto
è terminato senza eccezioni**. Se `onJoinPeerReceived` lancia sul remoto, gRPC
traduce l'eccezione in `UNKNOWN` e arriva `onError`: non si registra un peer che
non ci ha davvero registrati. Gratis.

Questo rende l'ordine dentro `CalibrationServiceImpl.joinP2P` un **invariante da
proteggere**:

```java
this.handler.onJoinPeerReceived(...);   // ① aggiorna lo stato
responseObserver.onNext(Empty...);      // ② solo dopo
responseObserver.onCompleted();         // ③ ACK
```

È questo ordine a rendere l'ACK significativo: quando arriva, **il remoto ci ha
già nella sua topologia**. Invertire ① e ③ per "rispondere subito e lavorare
dopo" romperebbe la garanzia in modo silenzioso.

### 5.4 Il latch, implementato a mano

Nessuna `java.util.concurrent.CountDownLatch`:

```java
final class Latch {
    private int count;

    Latch(int count) { this.count = count; }

    synchronized void countDown() {
        if (this.count > 0 && --this.count == 0) notifyAll();
    }

    /** @return true se il conteggio è arrivato a zero entro il timeout */
    synchronized boolean await(long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (this.count > 0) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) return false;
            wait(remaining);            // il while copre anche i risvegli spuri
        }
        return true;
    }
}
```

### 5.5 Il gate su `requestCalibration`

Senza questo, il fix non chiude il buco: finché `join` non è tornata,
`otherPeerCount()` è 0 e `requestCalibration` entra subito in `CalibratingState`
credendo di essere sola. Serve un flag `joined` controllato in ingresso.

Non serve invece un `JoiningState`: il comportamento *in ingresso* durante la
join è già corretto, perché si è in `IdleState`, che concede a chiunque chieda —
esattamente giusto per chi non sta ancora competendo. Aggiungere uno stato
richiederebbe di allargare `PeerState` con un metodo per il gate; il flag è più
onesto.

### 5.6 Bootstrap risultante

```java
grpcServer.start();                                     // 1. sii raggiungibile (Bug 2)
List<PeerInfo> known = restClient.registerPeer(me);     // 2. registrazione atomica (Bug 1)
boolean complete = algorithm.join(known, 5000);         // 3. handshake: entra chi ACKa
pl.start();                                             // 4. ora i sensori possono partire
```

### 5.7 Cosa risolve e cosa no

| Caso | Prima | Dopo |
|---|---|---|
| Il registry restituisce un peer già morto | entra nella topologia → quorum irraggiungibile | `onError` → mai registrato ✅ |
| Un peer esce mentre stiamo registrando | ghost nella topologia | `onError` o timeout → mai registrato ✅ |
| Il remoto è vivo ma il suo handler fallisce | non ce ne accorgiamo | `onError` → mai registrato ✅ |
| `addPeer` invocato due volte sullo stesso id | il vecchio channel resta aperto e perso | `commit` chiude il precedente ✅ |
| Due register concorrenti | entrambe si credono sole | risolto dal fix atomico dello Step 1 ✅ |
| **Il peer ACKa e poi crasha** | ghost | **ghost ancora** ❌ → Step 6 |

**Asimmetria voluta**: l'handshake protegge la join *in uscita*. Una join *in
arrivo* viene registrata subito, perché significa che il remoto ci ha già
committato — attendere una conferma di ritorno reintrodurrebbe il coordinamento
a due fasi che stiamo evitando.

L'ultima riga della tabella va detta esplicitamente: **l'handshake risolve la
race sulla join, non i crash successivi.** Sono due problemi ortogonali.

---

## Step 5-bis — Il bug del `Context` gRPC (trovato durante il test)

*Non era previsto: è emerso eseguendo quattro linee dopo lo Step 5.*

### Il sintomo

Quattro linee tutte bloccate in `WAITING`, per sempre. Nessun thread bloccato
in un thread dump, nessuna eccezione, nessun log. I grant venivano decisi e
spediti, ma non arrivavano mai.

### La causa

`io.grpc.Context` è un **thread-local** che porta deadline e cancellazione.
Mentre un handler del server gira, gRPC gli attacca il Context di **quella
chiamata**, e ogni RPC in uscita avviata su quel thread lo eredita come padre.
Quando la chiamata del server termina, quel Context viene cancellato, e con lui
tutti i figli.

```
1. il thread gRPC della linea 2 gestisce requestCalibration da 3
       [Context della chiamata ENTRANTE attaccato al thread]
2. onRequestReceived -> step() -> outbox: Grant->3 -> flush
3. transport.send(3, Grant)         <- RPC uscente, EREDITA quel Context
4. l'handler ritorna, responseObserver.onCompleted()
5. gRPC cancella il Context della chiamata entrante
6. il grant, ancora in volo, viene cancellato:
       CANCELLED: io.grpc.Context was cancelled without error
7. la linea 3 attende per sempre un grant che non arrivera' mai
```

Il bug **precede questo refactoring**: risale al commit `6fe42dd`
*"Removed anonymous threads in grpc service"*. Quei `new Thread(...)` non
servivano a parallelizzare: un thread nuovo non ha alcun Context attaccato,
quindi parte da `ROOT`. Stavano facendo, per caso, da isolamento del Context.

Restava invisibile perche' `ObserverFactory.emptyStreamObserver()` ingoiava
`onError`: il fallimento non compariva in nessun log.

### Il fix

```java
private void dispatch(int peerId, PeerChannel target, PeerMessage message) {
    final Context previous = Context.ROOT.attach();
    try {
        sendUnderRootContext(peerId, target, message);
    } finally {
        Context.ROOT.detach(previous);
    }
}
```

Non e' un workaround: e' la semantica corretta. Un grant di Ricart-Agrawala
**non e'** la risposta alla richiesta che lo ha innescato, e' una notifica
indipendente che viaggia su una chiamata separata in direzione opposta. Legarne
la vita a una chiamata entrante non correlata era il bug.

Sta nel transport perche' e' una preoccupazione specifica di gRPC, e copre ogni
percorso di invio da qualunque thread. Rimettere i thread funzionerebbe, ma
costa un thread per messaggio e **perde l'ordinamento**: due grant verso lo
stesso peer potrebbero invertirsi.

### Conseguenza permanente

L'observer di invio ora **logga** i fallimenti (`SEND FAILED to line N [...]`).
Un invio perso in silenzio si traduce in un peer che attende per sempre, e
nessun log che lo spieghi: e' il tipo di guasto che costa ore.

### Verifica

| Test | Risultato |
|---|---|
| 3 linee, 45s | 0 invii falliti, 3 sezioni critiche completate, tutte tornate IDLE |
| 4 linee + peer fantasma, soak 90s (180 campioni) | **max 1 linea in CALIBRATING per campione**, 0 invii falliti |
| Peer fantasma nella lista del registry | `JOIN REJECTED by line 99: UNAVAILABLE` -> quorum 3 e non 4, raggiungibile |
| Transizioni pubblicate su MQTT (45 messaggi, 4 linee) | **0 transizioni fuori sequenza** dopo il dispatcher ordinato |

---

## Step 6 — Migrazione a gRPC streaming

*Dettagliata in `GRPC_STREAMING_DESIGN.md`. Qui solo il punto di innesto.*

Dopo gli step 1–5 la migrazione tocca **quattro file** e nessuno di essi
appartiene all'algoritmo:

| File | Cambiamento |
|---|---|
| `smartfab.proto` | le 3 RPC di algoritmo → `rpc communicate(stream AlgorithmMessage) returns (Empty)`; `joinP2P` **resta unaria** (è un handshake) |
| `MessageCodec` | mappa su un `AlgorithmMessage` con `oneof` invece di 3 tipi distinti |
| `GrpcPeerTransport` | `send()` scrive sull'`outbound` persistente invece di aprire una RPC |
| `CalibrationServiceImpl` | 3 metodi override → 1 `communicate()` che ritorna uno `StreamObserver` |

```java
// PRIMA (unarie)
public void send(int peerId, PeerMessage msg) {
    var stub = stubOf(peerId);
    if (msg instanceof PeerMessage.Request r) stub.requestCalibration(codec.toProto(r), ignore());
    else if (msg instanceof PeerMessage.Grant g) stub.grantCalibrationAccess(codec.toProto(g), ignore());
    else if (msg instanceof PeerMessage.Leave l) stub.exitP2P(codec.toProto(l), ignore());
}

// DOPO (stream persistente)
public void send(int peerId, PeerMessage msg) {
    PeerChannel ch = channelOf(peerId);
    synchronized (ch.sendLock()) {              // outbound NON e' thread-safe
        ch.outbound().onNext(codec.toProto(msg));
    }
}
```

**Non cambiano**: `RicartEngine`, `PeerState` e le tre implementazioni,
`RicartContext`, `Outbox`, `RoundState`, `DeferredGrants`, `PeerRegistry`,
`ProductionLine`. Cioè tutto l'algoritmo.

Poiché la join resta una RPC unaria a sé, la direzione di risposta dello stream
non trasporta nulla: la forma corretta è **client-streaming**
(`returns (Empty)`), non bidirezionale. Di conseguenza l'header `x-peer-id` con
i due interceptor **resta necessario**, perché sullo stream non esiste un primo
messaggio garantito da cui ricavare l'id del mittente per l'`onError`.

Lo Step 6 abilita `onPeerUnreachable` (dichiarato già allo Step 4), che è
l'unica cosa che chiude il caso "ACKa e poi crasha".

### Il rischio da conoscere

Con la crash detection attiva, `onPeerUnreachable` toglie il peer dal quorum e
rivaluta subito `hasFullQuorum()`. Ma un failure detector su timeout produce
**falsi positivi**: una pausa di GC o un hiccup di rete può far scadere il
keepalive su un peer **vivo**. Se A dichiara morto B mentre B è vivo, entrambi
possono entrare in sezione critica: **safety violata**.

Oggi il problema non esiste perché i crash non vengono mai rilevati — il
fallimento attuale è di *liveness* (blocco indefinito). Lo Step 6 scambia un
problema di liveness con un rischio di safety. Mitigazioni realistiche:
timeout conservativi (30–60s), reset completo dello stato al rientro di un peer,
e dichiarazione esplicita del modello di fallimento assunto (crash-stop, niente
partizioni).

---

## 10. Tabella prima/dopo

| Aspetto | Oggi | Dopo |
|---|---|---|
| Punto di ingresso | `RicartMutualExclusionPeer`, 6 ruoli | `RicartEngine`, 1 stato esposto via 3 viste |
| IN vs OUT | mescolati in `MutualExclusionAlgorithm` | `PeerEventHandler` vs `MutualExclusionAlgorithm` |
| Protobuf | dentro l'algoritmo, in 4 punti | solo in `MessageCodec` |
| Membership | `Peer.addPeer` + `getAllPeersStub` | `PeerRegistry`, transport senza membership |
| Quando un peer conta | subito, non confermato | **solo dopo l'ACK di `joinP2P`** |
| I/O e lock | `send` dentro `synchronized` | `Outbox` drenata fuori dal lock |
| Testabilità degli stati | serve gRPC | `RicartContext` finto, zero dipendenze |
| MQTT | singleton nascosto in `setState` | `PeerStateChangedEvent` + `EventListener` |
| Pattern event-listener | due varianti (dispatcher + callback di stato) | uno solo, `EventListener<ProductionLineEvent>` |
| Consegna eventi | un thread per evento, ordine non garantito | una coda, un thread di dispatch, ordine garantito |
| Eventi | `extends EventDispatcher` + cast in `main` | composizione + `subscribe` sull'interfaccia |
| Registrazione al registry | read-then-write non atomico | `saveAndSnapshot` atomico |
| Ordine di avvio | register → gRPC server | gRPC server → register → join |

## Ordine di esecuzione

| Step | Comportamento | Verifica |
|---|---|---|
| 1 | **cambia** (fix) | due linee avviate insieme non calibrano entrambe |
| 2 | invariato | l'app gira esattamente come prima |
| 3 | invariato | idem |
| 4 | invariato | idem |
| 5 | **cambia** | un peer morto nella lista del registry non blocca il quorum |
| 6 | **cambia** | un peer che crasha viene rimosso dal quorum |
