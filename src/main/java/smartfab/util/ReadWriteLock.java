package smartfab.util;

/**
 * @author Gianluca Bianchi
 *
 *      Readers-writers lock built from nothing but the intrinsic monitor:
 *      synchronized, wait() and notifyAll(). No java.util.concurrent.
 *
 *      THE INVARIANT, and it is the whole class:
 *
 *          writerActive  =>  activeReaders == 0
 *          activeReaders >  0  =>  !writerActive
 *
 *      i.e. many readers together, or one writer alone, never both. The three
 *      counters are the entire state and are only ever touched inside the
 *      monitor, so a thread that wakes up always re-reads them.
 *
 *      WHY while() AND NOT if(). notifyAll() wakes EVERY waiter: readers and
 *      writers queue on the same monitor because Object gives one wait set per
 *      object. A woken thread has therefore not been told "the lock is yours",
 *      only "the state changed" — and by the time it re-acquires the monitor
 *      another thread may have taken what it was waiting for. It must re-test
 *      the condition, which is exactly what the loop does. An if() here is the
 *      textbook way to break mutual exclusion.
 *
 *      WHY notifyAll() AND NOT notify(). A single notify() picks one arbitrary
 *      waiter. Releasing a write lock has to be able to wake ALL the readers
 *      queued behind it — that is the point of a shared lock — and waking one
 *      reader that then goes back to sleep would lose the wake-up for everyone
 *      else. Worse, notify() could pick a reader while a writer is the only
 *      one that can proceed, and the system stalls with nobody running.
 *
 *      WRITER PREFERENCE. A reader also waits while waitingWriters > 0, not
 *      just while a writer is active. Without it a steady stream of readers
 *      keeps activeReaders permanently above zero and a writer never sees its
 *      condition become true: writer starvation. The price is the symmetric
 *      risk — under a flood of writers, readers wait. That is the right trade
 *      here: writes are one MQTT average per window per line, reads are
 *      occasional REST queries, so the writer queue drains and readers always
 *      get their turn.
 *
 *      NOT REENTRANT. A thread already holding the write lock that asks for it
 *      again waits for itself, forever. Never nest two acquisitions of the
 *      same lock — see the lock order documented in DynamicLockRepository.
 */
public final class ReadWriteLock {

    /** readers currently inside the critical section */
    private int     activeReaders;

    /** writers that asked for the lock and have not got it yet */
    private int     waitingWriters;

    /** true while the single writer is inside the critical section */
    private boolean writerActive;

    /**
     * Enters as a reader, waiting while a writer holds the lock or is queued
     * for it. Several readers pass this point at the same time.
     */
    public void acquireRead() {
        boolean interrupted = false;

        synchronized (this) {
            while (this.writerActive || this.waitingWriters > 0) {
                interrupted |= awaitChange();
            }
            this.activeReaders++;
        }

        restoreInterrupt(interrupted);
    }

    /**
     * Leaves as a reader.
     *
     * The wake-up only matters when the LAST reader leaves: a writer's
     * condition is activeReaders == 0, so waking it any earlier only makes it
     * re-check, find readers still inside and go back to sleep.
     */
    public synchronized void releaseRead() {
        this.activeReaders--;
        if (this.activeReaders == 0) {
            notifyAll();
        }
    }

    /**
     * Enters as the writer, waiting until nobody is inside — no readers and no
     * other writer.
     *
     * waitingWriters is incremented BEFORE waiting, so that readers arriving
     * in the meantime already see a writer queued and stop overtaking it.
     */
    public void acquireWrite() {
        boolean interrupted = false;

        synchronized (this) {
            this.waitingWriters++;
            while (this.writerActive || this.activeReaders > 0) {
                interrupted |= awaitChange();
            }
            this.waitingWriters--;
            this.writerActive = true;
        }

        restoreInterrupt(interrupted);
    }

    /** Leaves as the writer, waking readers and writers alike to compete. */
    public synchronized void releaseWrite() {
        this.writerActive = false;
        notifyAll();
    }

    /**
     * Must be called with the monitor held.
     *
     * Waiting is deliberately NOT interruptible. Bailing out of acquireWrite()
     * on an interrupt would return with waitingWriters already incremented and
     * never decremented: every future reader would then wait for a writer that
     * does not exist. So the interrupt is remembered and re-raised once the
     * lock has been acquired and the state is consistent again — the same
     * choice the JDK makes for an uninterruptible lock().
     *
     * @return true if this wait was interrupted
     */
    private boolean awaitChange() {
        try {
            wait();
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    private static void restoreInterrupt(boolean interrupted) {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
