package simpledb.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReadWriteLock {
    Set<TransactionId> holders;
    Map<TransactionId, Boolean> requests;// requester, isExclusive
    boolean exclusive;
    private int numberOfReaders;
    private int numberOfWriters;

    public ReadWriteLock() {
        this.holders = new HashSet<>();
        this.requests = new HashMap<>();
        this.exclusive = false;
        this.numberOfReaders = 0;
        this.numberOfWriters = 0;

    }

    public void readLock(TransactionId tid) {
        if (this.holders.contains(tid) && this.exclusive == false)
            return;
        this.requests.put(tid, false);
        synchronized (this) {
            try {
                while (numberOfWriters != 0) {
                    this.wait();
                }
                ++this.numberOfReaders;
                this.holders.add(tid);
                this.exclusive = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.requests.remove(tid);
    }

    public void writeLock(TransactionId tid) {
        if (this.holders.contains(tid) && this.exclusive)
            return;
        if (this.requests.containsKey(tid) && this.requests.get(tid))
            return;
        this.requests.put(tid, true);
        synchronized (this) {
            try {
                if (this.holders.contains(tid)) {
                    while (this.holders.size() > 1) {
                        this.wait();
                    }
                    this.releaseReadLockWithoutNoitfyAll(tid);
                }
                while (this.numberOfReaders != 0 || this.numberOfWriters != 0)
                    this.wait();
                ++numberOfWriters;
                this.holders.add(tid);
                this.exclusive = true;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        requests.remove(tid);
    }

    public void releaseReadLockWithoutNoitfyAll(TransactionId tid) {
        if (this.holders.contains(tid) == false) {
            return;
        } else {
            synchronized (this) {
                --this.numberOfReaders;
                this.holders.remove(tid);
            }
        }
    }

    public void releaseReadLock(TransactionId tid) {
        if (this.holders.contains(tid) == false) {
            return;
        }
        synchronized (this) {
            --this.numberOfReaders;
            this.holders.remove(tid);
            notifyAll();
        }
    }

    public void releaseWriteLock(TransactionId tid) {
        if (this.holders.contains(tid) == false || this.exclusive == false) {
            return;
        }
        synchronized (this) {
            --this.numberOfWriters;
            holders.remove(tid);
            notifyAll();
        }
    }

    public void releaseLock(TransactionId tid) {
        if (this.exclusive) {
            this.releaseWriteLock(tid);
        } else {
            this.releaseReadLock(tid);
        }
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public Map<TransactionId, Boolean> getRequests() {
        return this.requests;
    }

    public Set<TransactionId> getRequesters() {
        return this.requests.keySet();
    }

    public Set<TransactionId> getHolders() {
        return this.holders;
    }

    public boolean isHolder(TransactionId tid) {
        return this.holders.contains(tid);
    }
}