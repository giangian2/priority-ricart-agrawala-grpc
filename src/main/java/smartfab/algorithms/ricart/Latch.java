package smartfab.algorithms.ricart;

/**
 * @author Gianluca Bianchi
 *
 *      Minimal count-down latch built on the intrinsic monitor, so that the
 *      project does not depend on java.util.concurrent.
 *
 *      One-shot: once the count reaches zero it stays there and every await
 *      returns immediately.
 */
public final class Latch {

    private int count;

    public Latch(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        this.count = count;
    }

    /**
     * Decrements the count, waking up every waiter when it reaches zero.
     */
    public synchronized void countDown() {
        if (this.count > 0 && --this.count == 0) {
            notifyAll();
        }
    }

    /**
     * Waits until the count reaches zero or the timeout expires.
     *
     * The wait sits inside a while loop because wait() can return without a
     * matching notify (spurious wakeup) and because a notify aimed at another
     * waiter can wake us too.
     *
     * @param timeoutMillis maximum time to wait
     * @return true if the count reached zero, false if the deadline expired
     */
    public synchronized boolean await(long timeoutMillis) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMillis;

        while (this.count > 0) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return false;
            }
            wait(remaining);
        }
        return true;
    }
}
