package simpledb.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

public class LockManager {
  // Essentially, LockManager tracks which locks each transactions holds and checks if locks should be granted to a transaction
  // lock
  // unlock
  // track locks held by each transaction
  // grant locks to transactions

  // S(A)-->shared lock --> read
  // X(A)-->exclusive lock --> read and write
  // U(A)-->release lock --> when the transaction commit
  // https://sutd50043.github.io/notes/l9_transaction_concurrency/

  // Map to store locks associated with each page
  Map<PageId, ReadWriteLock> pageLockManager;

  // Dependency graph to track dependencies between transactions to detect deadlocks
  Map<TransactionId, Set<TransactionId>> dependencyGraph;

  // Map to keep track of pages held by each transaction
  Map<TransactionId, Set<PageId>> pagesHeldByTransactionId;

  public LockManager() {
    // Initialize the data structures to store locks, dependencies and pages held by each transaction
      pageLockManager = new ConcurrentHashMap<>();
      dependencyGraph = new ConcurrentHashMap<>();
      pagesHeldByTransactionId = new ConcurrentHashMap<>();
  }

  public void obtainReadLock(TransactionId tid, PageId pid)
  throws TransactionAbortedException {

    ReadWriteLock lock;

    // Check if the transaction already holds the lock
    synchronized (this) {

      // Obtaining lock associated with page id (or creating it)
      lock = obtainOrCreateLock(pid);

      // If the transaction already holds the lock, return and exit function
      if (lock.isHolder(tid)) return;

      // If the lock is held by another transaction and is exclusive, there could be potential deadlock situation and need to account for that
      if (!lock.getHolders().isEmpty() && lock.isExclusive()) {

        // Add the transaction to the dependency graph
        dependencyGraph.put(tid, lock.getHolders());

        // If deadlock exists
        if (isDeadLock(tid)) {

          // Remove the transaction from the dependency graph and abort
          dependencyGraph.remove(tid);
          throw new TransactionAbortedException();
        }
      }
    }    

    // Acquire the read lock on the page for given transaction
    lock.readLock(tid);

    synchronized (this) {
      // Remove the transaction from the dependency graph and add page to set of pages held by transaction
      dependencyGraph.remove(tid);
      obtainOrCreatePagesHeld(tid).add(pid);
    }
  }

  public void obtainWriteLock(TransactionId tid, PageId pid)
  throws TransactionAbortedException {
    
    ReadWriteLock lock;

    synchronized (this) {

      // Obtaining lock associated with page id (or creating it)
      lock = obtainOrCreateLock(pid);

      // If the current transaction id already holds an exclusive lock, return and exit function
      if (lock.isExclusive() && lock.isHolder(tid))
          return;

      // If the lock is held by another transaction, check for potential deadlock
      if (!lock.getHolders().isEmpty()){

        // Add the transaction to the dependency graph
        dependencyGraph.put(tid, lock.getHolders());

        // Assert if tid is in dependencyGraph

        // If deadlock exists
        if (isDeadLock(tid)) {
          // Remove the transaction from the dependency graph and abort
          dependencyGraph.remove(tid);
          throw new TransactionAbortedException();
        }
      }
    }

    // Acquires a write lock on the page for the given transaction (tid)
    lock.writeLock(tid);

    synchronized (this) {
      // Remove the transaction from the dependency graph and add page to set of pages held by transaction
      dependencyGraph.remove(tid);
      obtainOrCreatePagesHeld(tid).add(pid);
    }
  }


  public synchronized void releaseLock(TransactionId tid, PageId pid) {

    // If page id is not present in the map, there is no locks associated with it and we return early
    if (!pageLockManager.containsKey(pid)) return;

    // Retrieves the lock associated with the page id
    ReadWriteLock lock = pageLockManager.get(pid);

    // Release the lock
    lock.releaseLock(tid);

    // Removes the specified page (pid) from the set of pages held by the transaction.
    pagesHeldByTransactionId.get(tid).remove(pid);
  }

  public synchronized void releaseAllLocks(TransactionId tid) {

    // If the transaction id is not present in the map, there are no locks associated with it and we return early
    if (!pagesHeldByTransactionId.containsKey(tid)) return;

    // Retrieves the set of pages held by the transaction
    Set<PageId> pages = pagesHeldByTransactionId.get(tid);

    // For each page in the set of pages held by the transaction, release the lock
    for (Object pid : pages.toArray()) {
      releaseLock(tid, (PageId) pid);
    }

    // Removes the transaction id from the map
    pagesHeldByTransactionId.remove(tid);
  } 

  private synchronized ReadWriteLock obtainOrCreateLock(PageId pid) {
    // If the page id is not present in the map, create a new lock and add it to the map
    if (!pageLockManager.containsKey(pid)) {
      pageLockManager.put(pid, new ReadWriteLock());
    }

    // Return the lock associated with the page id
    return pageLockManager.get(pid);
  }


  private synchronized Set<PageId> obtainOrCreatePagesHeld(TransactionId tid) {
    // If the transaction id is not present in the map, create a new set and add it to the map
    if (!pagesHeldByTransactionId.containsKey(tid)) {
      pagesHeldByTransactionId.put(tid, new HashSet<PageId>());
    }

    // Return the set of pages held by the transaction
    return pagesHeldByTransactionId.get(tid);
  }

  
  private boolean isDeadLock(TransactionId tid) {
    Set<TransactionId> visitedTransactions = new HashSet<>();
    Queue<TransactionId> transactionQueue = new LinkedList<>();
    visitedTransactions.add(tid);
    transactionQueue.offer(tid);
    
    while (!transactionQueue.isEmpty()) {
      TransactionId currentTransaction = transactionQueue.poll();
        
      if (!dependencyGraph.containsKey(currentTransaction)) 
        continue;
        
      assert !dependencyGraph.get(currentTransaction).isEmpty();
      for (TransactionId adjacentTransaction : dependencyGraph.get(currentTransaction)) {
          if (adjacentTransaction.equals(currentTransaction)) 
            continue;

          if (!visitedTransactions.contains(adjacentTransaction)) {
            visitedTransactions.add(adjacentTransaction);
            transactionQueue.offer(adjacentTransaction);
          } else {
            // Deadlock detected!
            return true;
          }
      }
    }
    return false;
  }

  public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    if (!pagesHeldByTransactionId.containsKey(tid)) return false;
    return pagesHeldByTransactionId.get(tid).contains(pid);
  }

  public Set<PageId> getPagesHeldBy(TransactionId tid) {
    if (pagesHeldByTransactionId.containsKey(tid))
      return pagesHeldByTransactionId.get(tid);
    return null;
  }
}
