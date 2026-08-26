# `io.grpc.Context`, cancellazione e il bug dei grant fantasma

Documento di riferimento sul meccanismo che ha causato il blocco descritto in
`ARCHITECTURE_REFACTOR.md` (Step 5-bis) e sul perché la correzione è
`detached()` in `GrpcPeerTransport`.

L'obiettivo è arrivare al livello dei frame HTTP/2 e dei thread, perché il bug
è invisibile finché si ragiona al livello "chiamo il metodo, il messaggio
parte".

---

## Indice

1. [Il malinteso di partenza](#1-il-malinteso-di-partenza)
2. [Che cosa è `Context`](#2-che-cosa-è-context)
3. [Lato server: chi attacca il Context al thread](#3-lato-server-chi-attacca-il-context-al-thread)
4. [Lato client: quando e cosa viene ereditato](#4-lato-client-quando-e-cosa-viene-ereditato)
5. [Il bug, con due thread e i frame veri](#5-il-bug-con-due-thread-e-i-frame-veri)
6. [Il canale freddo: `DelayedClientTransport`](#6-il-canale-freddo-delayedclienttransport)
7. [Il canale caldo: `WriteQueue` e `RST_STREAM`](#7-il-canale-caldo-writequeue-e-rst_stream)
8. [Perché i thread anonimi lo nascondevano](#8-perché-i-thread-anonimi-lo-nascondevano)
9. [Il fix: `detached()`](#9-il-fix-detached)
10. [Conseguenze per la migrazione a streaming](#10-conseguenze-per-la-migrazione-a-streaming)
11. [Tassonomia degli errori vicini](#11-tassonomia-degli-errori-vicini)
12. [Come osservarlo dal vivo](#12-come-osservarlo-dal-vivo)

---

## 1. Il malinteso di partenza

La domanda naturale davanti a questo bug è:

> Il thread, prima di terminare, ha inviato la richiesta. Come fa un invio a
> essere interrotto a metà?

La risposta è che **non viene interrotto a metà: nella maggior parte dei casi
non era ancora partito.** La premessa nascosta è

```
lo stub ha ritornato  ⇒  il messaggio è sulla rete
```

e questa premessa è falsa. Smontarla è il 90% della spiegazione.

### Cosa fa davvero lo stub asincrono

`stub.grantCalibrationAccess(msg, observer)` si espande, dentro
`io.grpc.stub.ClientCalls`, in questo:

```java
ClientCall<CalibrationRequest, Empty> call = channel.newCall(METHOD, callOptions);

call.start(listener, new Metadata());   // 1. procura un transport, crea lo stream
call.sendMessage(request);              // 2. serializza e ACCODA
call.halfClose();                       // 3. accoda END_STREAM
call.request(2);                        // 4. dichiara quante risposte accetti
// ritorna immediatamente
```

I quattro passi girano davvero in modo sincrono sul thread chiamante. Ma
**nessuno dei quattro scrive sul socket.**

In `grpc-netty` la scrittura passa da una `WriteQueue`, e ogni frame diventa un
comando accodato:

```java
// NettyClientStream.Sink.writeFrame(...)
writeQueue.enqueue(new SendGrpcFrameCommand(stream, bytebuf, endOfStream), flush);
```

Chi svuota quella coda e scrive sul file descriptor è il **netty event loop**,
cioè un altro thread. Il chiamante ha soltanto depositato del lavoro.

> **Regola da tenere:** il ritorno di uno stub asincrono significa *"accodato"*,
> mai *"trasmesso"*. Sono due thread e due istanti diversi.

---

## 2. Che cosa è `Context`

Va distinto da due cose con cui viene regolarmente confuso:

| Tipo | Contenuto | Viaggia sulla rete? | Scope |
|---|---|---|---|
| `Metadata` | header HTTP/2 (`x-peer-id`, `authorization`…) | **sì** | una chiamata |
| `CallOptions` | deadline, executor, compressore, credenziali | no | una chiamata |
| **`Context`** | **cancellazione, deadline, valori** | **no** | tutto il lavoro discendente, anche su più thread |

`Context` non trasporta dati applicativi: trasporta il **diritto di interrompere
del lavoro**. Tre proprietà spiegano tutto il resto.

### 2.1 È immutabile e ad albero

Non si modifica un `Context`, se ne deriva uno figlio:

```java
Context parent = Context.current();
Context child  = parent.withValue(USER_KEY, "gianluca");     // nuovo oggetto
Context.CancellableContext cancellable = parent.withCancellation();
Context withDeadline = parent.withDeadlineAfter(5, TimeUnit.SECONDS, scheduler);
```

La radice è `Context.ROOT`. Cancellare un nodo cancella **tutto il sottoalbero**.

### 2.2 Vive in un `ThreadLocal`

`Context.current()` legge da un thread-local (`ThreadLocalContextStorage`).
`attach()` ci scrive e restituisce il precedente; `detach()` ripristina:

```java
Context previous = myContext.attach();   // thread-local ← myContext
try {
    // qui Context.current() == myContext
} finally {
    myContext.detach(previous);          // thread-local ← previous
}
```

Non c'è nient'altro. Non è magia, è una variabile per thread.

### 2.3 Il thread-local NON è ereditato dai thread figli

È un `ThreadLocal` normale, **non** un `InheritableThreadLocal`. Dimostrazione
minima, che si può incollare in un `main`:

```java
public static void main(String[] args) throws Exception {
    Context.Key<String> K = Context.key("k");
    Context ctx = Context.current().withValue(K, "sono nel context");

    Context previous = ctx.attach();
    try {
        System.out.println("thread corrente : " + K.get());      // "sono nel context"

        Thread t = new Thread(() -> 
            System.out.println("thread figlio  : " + K.get()));  // null !
        t.start();
        t.join();

        System.out.println("current == ROOT nel figlio? lo è, per costruzione");
    } finally {
        ctx.detach(previous);
    }
}
```

Un thread appena creato parte sempre da `ROOT`. Ricordarselo: è il punto 8.

### 2.4 Solo un `CancellableContext` può morire

```java
Context.ROOT.canBeCancelled()                  // false
Context.current().withCancellation()           // → CancellableContext, true
```

E, decisivo per il fix, `Context.addListener` comincia così:

```java
public void addListener(CancellationListener listener, Executor e) {
    if (!canBeCancelled()) {
        return;          // ← su ROOT esce subito: nessun listener registrato
    }
    ...
}
```

---

## 3. Lato server: chi attacca il Context al thread

Quando arriva una RPC, prima di invocare l'handler applicativo gRPC fa, in
sostanza:

```java
// semplificazione di ServerImpl / ServerCallImpl
Context.CancellableContext ctxIn = ROOT.withCancellation();   // + deadline del client
Context previous = ctxIn.attach();
try {
    service.requestCalibration(request, responseObserver);    // il TUO handler
} finally {
    ctxIn.detach(previous);
}
```

Quindi, mentre gira `CalibrationServiceImpl.requestCalibration`,
`Context.current()` **non è ROOT**: è un `CancellableContext` che rappresenta la
chiamata entrante. Serve a farti sapere se il client ha mollato.

### Il momento decisivo: la chiusura cancella

Quando la chiamata server si chiude, gRPC cancella quel context. **Anche quando
è andata a buon fine:**

```java
// ServerImpl$ServerCallStreamListenerImpl.closed(Status status)
ctxIn.cancel(status.isOk() ? null : status.getCause());
```

E la descrizione dell'errore nasce qui:

```java
// io.grpc.Contexts.statusFromCancelled(Context context)
Throwable cause = context.cancellationCause();
if (cause == null) {
    return Status.CANCELLED.withDescription("io.grpc.Context was cancelled without error");
}
```

**Ecco da dove viene "without error".** Non è un fallimento: è il normale
fine-vita di una chiamata *riuscita*. La cancellazione è il modo in cui gRPC
dice "questo scope è chiuso, chi ci stava dentro smetta". Il problema è chi ci è
finito dentro senza volerlo.

---

## 4. Lato client: quando e cosa viene ereditato

`ClientCallImpl`, nel costruttore, fotografa il thread-local:

```java
this.context = Context.current();     // ⚠️ ADESSO, non alla risposta
```

e in `start()` si iscrive alla sua cancellazione:

```java
context.addListener(cancellationListener, directExecutor());
// se il context viene cancellato →  this.cancel(...)  →  RST_STREAM sullo stream
```

Due conseguenze da tenere ferme:

**a)** La cattura avviene **nell'istante in cui chiami il metodo dello stub**.
Se in quel momento il thread porta `ctxIn`, il legame è già stretto e non lo
sciogli più: non c'è API per riparentare una call dopo la creazione.

**b)** Se il context catturato è `ROOT`, `addListener` esce subito (§2.4) e
**nessun listener viene registrato**: nulla, mai, potrà cancellare quella call
dall'esterno. Questo è il fix, meccanicamente.

---

## 5. Il bug, con due thread e i frame veri

Scenario: la linea 2 riceve una `requestCalibration` dalla linea 3 e deve
risponderle con un grant.

```
THREAD A — grpc server thread della linea 2        THREAD B — netty event loop
─────────────────────────────────────────────      ─────────────────────────────

t0  netty legge HEADERS+DATA in ingresso
t1  ctxIn = CancellableContext(requestCalibration da 3)
t2  ctxIn.attach()          ← thread-local = ctxIn
t3  CalibrationServiceImpl.requestCalibration(...)
t4    handler.onRequestReceived(...)
t5    RicartEngine.step():
        [monitor] decide grant → outbox.to(3, Grant)
        [fuori]   flush() → transport.send(3, Grant)
t6      stub.grantCalibrationAccess(...)
          new ClientCallImpl:
            context = Context.current() = ctxIn      ⚠️
          start():
            ctxIn.addListener(cancella questa call)  ⚠️
          sendMessage + halfClose:
            ACCODA i frame                     ────────►  (coda: HEADERS, DATA, END_STREAM)
t7    l'handler ritorna
t8  responseObserver.onNext(Empty)
t9  responseObserver.onCompleted()
t10 gRPC chiude lo stream server
      ctxIn.cancel(null)
t11   il listener di t6 scatta
        ClientCallImpl.cancel(...)
        stream.cancel(CANCELLED)         ────────►  (coda: … + RST_STREAM)
t12 CANCELLED: io.grpc.Context was
    cancelled without error                          ??? ← e qui dipende, §6 e §7
```

Il difetto è tutto in **t6**: una notifica indipendente si è ritrovata come
genitore la chiamata che l'aveva solo *innescata*.

Semanticamente è sbagliato. Il grant **non è la risposta** alla request: la
risposta alla request è un `Empty` che nessuno legge (vedi `replyOf` in
`GrpcPeerTransport`). Il grant è una RPC nuova, in direzione opposta, la cui
vita non ha niente a che vedere con lo scambio che l'ha provocata.

Quello che succede a `t12` dipende dallo stato del canale, e sono due storie
diverse.

---

## 6. Il canale freddo: `DelayedClientTransport`

`ManagedChannelBuilder.build()` **non compone nessun socket**: il canale nasce
in stato `IDLE`. La prima RPC lo fa uscire dall'idle e innesca:

```
IDLE ──(prima RPC)──► CONNECTING ──► READY
                       │
                       ├─ name resolution
                       ├─ TCP three-way handshake (SYN → SYN-ACK → ACK)
                       └─ HTTP/2 preface + SETTINGS exchange
```

Finché il canale non è `READY` non esiste nessun transport reale, quindi gRPC
parcheggia la chiamata in un **`DelayedClientTransport`**: la `ClientCall`
esiste, ma il suo stream è un `DelayedStream`, un oggetto che **bufferizza in
memoria** aspettando la connessione.

Situazione reale a `t6`:

```
grant → DelayedStream (buffer in RAM) → DelayedClientTransport.pendingStreams

nessun socket, nessun frame, nessun byte
```

A `t11`, `cancel()` su un `DelayedStream` fa una cosa sola: **lo toglie dalla
lista dei pendenti** e notifica `CANCELLED` al listener.

**Il grant muore in una coda in memoria, prima che esista una connessione TCP.**
Non c'è nessuna interruzione a metà: non è mai partito, e sul filo non è
comparso un solo byte.

### Perché era proprio questo il caso che colpiva

Il canale freddo non era un caso raro: era **strutturale** in una direzione
precisa. Quando un peer si annuncia a noi:

```
joinP2P  (handler server → ctxIn attaccato al thread)
  └─ onJoinPeerReceived
       ├─ registry.add(newcomer)
       ├─ transport.connect(newcomer)          ← canale NUOVO, mai usato, IDLE
       └─ se sono in WAITING:
            outbox.to(senderId, Request)       (RicartEngine)
  └─ step() esce dal monitor e flusha
       └─ send(newcomer, Request)              ← PRIMA RPC su quel canale
  └─ l'handler ritorna → onCompleted()
  └─ ctxIn.cancel(null) → la Request pendente viene scartata
```

Canale mai usato **più** thread che porta il context della chiamata entrante:
le due condizioni peggiori insieme, per costruzione, sempre. Qui il fallimento
era garantito, non probabilistico.

Verso i peer a cui eravamo stati *noi* a fare join, invece, il canale era già
stato scaldato dalla `joinP2P` uscente: era `READY`, e i grant spesso passavano.
Da qui l'apparente intermittenza, che faceva sembrare il problema un difetto
dell'algoritmo.

---

## 7. Il canale caldo: `WriteQueue` e `RST_STREAM`

Con il canale `READY` la `WriteQueue` esiste ed è **FIFO**:

```
THREAD A (handler)                          THREAD B (netty event loop)
enqueue HEADERS
enqueue DATA
enqueue END_STREAM
  → ritorna dallo stub
onCompleted() → ctxIn.cancel(null)
enqueue CancelClientStreamCommand
                                            drena in ordine:
                                              → HEADERS
                                              → DATA
                                              → END_STREAM
                                              → RST_STREAM
```

Siccome il cancel è accodato **dopo**, i dati vengono scritti per primi. Quindi
in questo caso i byte del grant escono davvero, seguiti da un `RST_STREAM`.

Cosa ne segue:

- **Sul peer ricevente**: se il `DATA` viene letto e consegnato all'handler
  prima che arrivi l'`RST_STREAM`, il grant è processato normalmente. Se l'RST
  arriva prima, la call viene scartata. È una corsa che dipende dalla rete.
- **Sul mittente**: **in ogni caso** la `ClientCall` viene chiusa con
  `CANCELLED` e `onError` viene invocato. Cioè è possibilissimo trovarsi nella
  situazione "il grant è arrivato *e* io ho loggato un errore".

### `RST_STREAM` e lo stato `CLOSED`

`ClientCall.cancel()` su un canale attivo si traduce in un frame **`RST_STREAM`**
sullo stream HTTP/2 di quella RPC. Nella macchina a stati di HTTP/2:

```
idle ──HEADERS──► open ──END_STREAM──► half-closed ──► closed
                    │
                    └──────── RST_STREAM ──────────► CLOSED   (immediato, bilaterale)
```

`RST_STREAM` porta lo stream direttamente in **`CLOSED`** da entrambi i lati,
**senza toccare la connessione TCP né gli altri stream** multiplexati su di
essa. È il meccanismo con cui HTTP/2 aborta una singola richiesta preservando la
connessione.

Sopra a quel frame, gRPC mappa l'evento nello status `CANCELLED` (codice 1).

### Riassunto dei due esiti

| Stato del canale | Il messaggio | Cosa si osserva |
|---|---|---|
| `IDLE` / `CONNECTING` | **mai trasmesso**, scartato dal buffer in RAM | `CANCELLED`, e il peer attende per sempre |
| `READY` | trasmesso, seguito da `RST_STREAM`; il peer *forse* lo processa | `CANCELLED`, ma spesso funzionava lo stesso |

La seconda riga spiega perché il bug non bloccava sempre. La prima spiega perché,
quando bloccava, bloccava in modo definitivo e silenzioso.

---

## 8. Perché i thread anonimi lo nascondevano

Prima del commit `6fe42dd` (*"Removed anonymous threads in grpc service"*) ogni
handler faceva `new Thread(...)`. Non servivano a parallelizzare, ed è per
questo che sembrava giusto toglierli.

Ma per §2.3, **`Context` non usa `InheritableThreadLocal`**: un thread appena
creato ha il thread-local vuoto, quindi `Context.current()` è `ROOT` per
definizione. Quei thread stavano facendo, **per puro effetto collaterale**,
esattamente ciò che `detached()` fa oggi di proposito.

Rimuoverli non ha introdotto il bug: lo ha **smascherato**. Il difetto — un
grant che eredita lo scope della request — c'era già, mascherato da un dettaglio
implementativo che nessuno aveva messo lì per quel motivo.

È la ragione per cui il fix deve essere esplicito e commentato: una protezione
accidentale non sopravvive al primo refactoring che la incontra.

### Il secondo strato di silenzio

Il fallimento non compariva in nessun log per due motivi sovrapposti:

1. `ObserverFactory.emptyStreamObserver()` implementava `onError` con corpo
   vuoto: il `CANCELLED` arrivava e veniva ingoiato.
2. La cancellazione **non lancia niente sul thread chiamante**. Lo stub
   asincrono è già ritornato; l'esito viaggia solo attraverso lo
   `StreamObserver` di risposta. Nessuna eccezione, nessun thread bloccato in un
   thread dump. Quattro processi vivi e un algoritmo fermo.

È il motivo per cui `replyOf` oggi stampa in `onError`.

---

## 9. Il fix: `detached()`

```java
private static void detached(Runnable outgoingCall) {
    final Context previous = Context.ROOT.attach();
    try {
        outgoingCall.run();
    } finally {
        Context.ROOT.detach(previous);
    }
}
```

Meccanicamente, passo per passo:

1. `ROOT.attach()` scrive `ROOT` nel thread-local e restituisce `ctxIn`.
2. Dentro il blocco, `stub.grantCalibrationAccess(...)` costruisce la
   `ClientCallImpl`, che fotografa `Context.current()` → ottiene `ROOT`.
3. `start()` chiama `ROOT.addListener(...)`, che **esce subito senza registrare
   nulla** perché `ROOT` non è cancellabile (§2.4).
4. Nessun genitore, nessun listener: nulla può uccidere quella call. Vive finché
   non finisce da sola o finché non muore il canale.
5. Il `finally` rimette `ctxIn` al suo posto.

### Il `finally` non è igiene, è obbligatorio

I worker di gRPC appartengono a un pool e vengono **riusati**. Lasciare `ROOT`
attaccato significherebbe che il prossimo lavoro schedulato su quel thread parte
senza il *suo* context, e quindi **senza la sua deadline**, che smetterebbe
silenziosamente di essere applicata. gRPC per giunta rileva l'attach/detach
sbilanciato e logga a livello `SEVERE`, ma il danno sarebbe già fatto.

### Due cose che `detached()` NON fa

- **Non rende la chiamata asincrona.** Lo stub è già `newStub()`, cioè
  asincrono. Il detach cambia la *parentela*, non il modello di esecuzione.
- **Non fa perdere una cancellazione utile.** Ereditare avrebbe senso se il
  lavoro uscente fosse *al servizio* della chiamata entrante — il caso tipico è
  un gateway che interroga un backend per costruire la risposta: lì, se il
  client se ne va, è giusto abortire tutto. Qui la relazione non esiste: è un
  peer che ne notifica un altro. `ROOT` non è una scappatoia, è la dichiarazione
  corretta della semantica.

### Perché ci passa anche `joinP2P`, che non era rotta

`joinNetwork` gira sul thread di bootstrap, dove `Context.current()` è già
`ROOT`: quel percorso non è mai stato rotto. Ci passa comunque per mantenere un
**invariante verificabile** — nessuna `stub().<rpc>` di quella classe sfugge al
detach — invece di un commento sui call site che diventa falso il giorno in cui
qualcuno innesca un rejoin da `onPeerUnreachable`, che invece gira su un thread
gRPC. Il costo su quel percorso è due letture di thread-local, una volta sola
all'avvio.

---

## 10. Conseguenze per la migrazione a streaming

Questo è il punto operativo, perché `GRPC_STREAMING_DESIGN.md` §10 lo rende
imminente.

**Oggi** `connect()` non avvia nessuna RPC: `openChannel()` fa `build()` +
`newStub()`, che non toccano la rete. Nessuna `ClientCall`, nessun context
catturato. Per questo `connect()` non passa da `detached()` e non è un problema.

**Con lo stream persistente** cambia tutto:

```
onJoinPeerReceived  ←  CalibrationServiceImpl.joinP2P  ←  thread server, ctxIn attaccato
     └─ transport.connect(newcomer)
            └─ stub.communicate(inbound)   ← RPC VERA: cattura Context.current() = ctxIn
```

Lo **stream long-lived verso quel peer** erediterebbe il context della `joinP2P`
entrante e verrebbe cancellato appena quell'handler chiude la risposta, cioè
dopo qualche millisecondo.

Non si perderebbe un messaggio: si perderebbe **l'intero canale verso quel
peer**, appena aperto, per sempre — e in silenzio, come prima.

> **Prerequisito della migrazione:** `connect()` deve stare dentro `detached()`
> esattamente come `send()`. È un motivo *indipendente*, e in aggiunta, rispetto
> al vincolo "non deve bloccare sotto il lock dell'algoritmo" documentato in
> `RicartEngine` e nel README.

---

## 11. Tassonomia degli errori vicini

Errori che questo codice può produrre e che è facile confondere fra loro:

| Messaggio | Origine reale |
|---|---|
| `CANCELLED: io.grpc.Context was cancelled without error` | Il bug di questo documento: RPC uscente figlia di una entrante che si è chiusa. Risolto da `detached()`. |
| `UNAVAILABLE: Channel shutdownNow invoked` | `attempt.shutdownNow()` nel timeout del join, in `joinNetwork`. Atteso. |
| `UNAVAILABLE: Stream closed before write could take place` | Canale chiuso mentre una `send` era in volo: es. `disconnect()` da `forgetPeer` mentre un altro thread sta flushando l'outbox. Benigno oggi, il peer è comunque andato. |
| `IllegalStateException: call already closed` | `onNext`/`onCompleted` chiamati due volte sullo stesso `responseObserver`. Non accade oggi; **diventerà possibile con lo streaming**, dove l'observer è condiviso e long-lived: è il motivo per cui `PeerChannel.sendLock` esiste già. |
| `UNAVAILABLE: io exception` / `Connection refused` | Peer morto o non ancora in ascolto. È il caso che `onPeerUnreachable` deve gestire. |
| `DEADLINE_EXCEEDED` | Non usato qui: nessuna `withDeadlineAfter` sulle RPC dell'algoritmo. Se comparisse, verrebbe da un context ereditato con deadline — cioè, di nuovo, da un detach mancante. |

---

## 12. Come osservarlo dal vivo

Il modo più diretto è il logging interno di gRPC, che stampa le transizioni di
stato del canale e i frame. Con un `logging.properties`:

```properties
handlers = java.util.logging.ConsoleHandler
.level = INFO

java.util.logging.ConsoleHandler.level = FINEST
java.util.logging.ConsoleHandler.formatter = java.util.logging.SimpleFormatter

io.grpc.level = FINE
io.grpc.netty.level = FINE
```

e lanciando con:

```
-Djava.util.logging.config.file=logging.properties
```

Cosa cercare:

- le transizioni `IDLE → CONNECTING → READY` del canale verso un peer,
- l'istante in cui la `ClientCall` viene creata rispetto a quello in cui il
  canale diventa `READY`,
- l'evento di cancellazione del context della chiamata entrante.

Il confronto interessante è fra il comportamento su `6fe42dd` (fallimento
riproducibile con più linee che si uniscono in sequenza) e su `HEAD`.

Alternativa senza modificare il codice: `tcpdump`/Wireshark con il dissector
HTTP/2 su `localhost`, dove si vedono direttamente i `HEADERS`, i `DATA` e — nel
caso caldo — il `RST_STREAM` che li segue.

---

## Riassunto in cinque righe

1. Uno stub asincrono **accoda**, non trasmette; a trasmettere è il netty event
   loop, un altro thread.
2. Una `ClientCall` fotografa `Context.current()` **alla creazione** e muore con
   esso.
3. Un handler server gira con il context della chiamata entrante attaccato, e
   quel context viene **cancellato alla chiusura della risposta**, anche in caso
   di successo.
4. Quindi un grant accodato dentro un handler moriva prima di partire: con
   canale freddo non usciva un solo byte.
5. `detached()` fa sì che la call fotografi `ROOT`, che non è cancellabile:
   nessun listener, nessun genitore, vita indipendente. Che è la verità su cosa
   sono questi messaggi.
