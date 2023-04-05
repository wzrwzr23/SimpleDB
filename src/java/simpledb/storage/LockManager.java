package simpledb.storage;

import simpledb.common.Permissions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class LockManager {
    // key：pid; value: list of locks of the page
    private Map<PageId, List<PageLevelLock>> lockMap;
    private long timeout = 500;

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
    }

    /**
     * transaction acquire page level lock 
     * throw TransactionAbortedException when timeout
     * avoid deadlock
     * 
     * @param tid
     * @param pid
     * @param perm
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException{

        boolean locked = false;
        long start = System.currentTimeMillis();
        // long timeout = new Random().nextInt(2000);
        while(!locked){
            // long now = System.currentTimeMillis();
            if(System.currentTimeMillis() - start> timeout){
                throw new TransactionAbortedException();
            }
            locked = acquireLockHelper(tid, pid, perm);
        }
    }

    /**
     * transaction acquire lock helper function
     * 
     * @param tid
     * @param pid
     * @param perm
     */
    public synchronized boolean acquireLockHelper(TransactionId tid, PageId pid, Permissions perm){
        PageLevelLock lock = new PageLevelLock(tid, perm);
        List<PageLevelLock> locks = lockMap.get(pid);
        if(locks == null || locks.size() == 0){
            locks = new ArrayList<>();
            locks.add(lock);
            lockMap.put(pid,locks);
            return true;
        }

        // only one transaction
        if(locks.size() == 1){
            PageLevelLock curLock = locks.get(0);
            if(curLock.getTransactionId().equals(tid)){
                // upgrade lock or not
                if(curLock.getPermissions().equals(Permissions.READ_ONLY) 
                    && perm.equals(Permissions.READ_WRITE)){
                    curLock.setPermissions(Permissions.READ_WRITE);
                }
                return true;
            }else{
                // add READ lock
                if(curLock.getPermissions().equals(Permissions.READ_ONLY) 
                && perm.equals(Permissions.READ_ONLY)){
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }

        // multiple transactions, which means they must be READ_ONLY
        if(!perm.equals(Permissions.READ_ONLY)){
            return false;
        }
        
        for(PageLevelLock l: locks){
            // transaction already holds the lock
            if(l.getTransactionId().equals(tid)){
                return true;
            }
        }
        locks.add(lock);
        return true;
    }

    /**
     * release lock of the transation for given page
     * 
     * @param tid
     * @param pid
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid){
        if (lockMap.containsKey(pid)){
            List<PageLevelLock> locks = lockMap.get(pid);
            for(PageLevelLock l: locks){
                if(l.getTransactionId().equals(tid)){
                    locks.remove(l);
                    return;
                }
            }
        }    
    }

    /**
     * release all locks of the transaction
     * 
     * @param tid
     */
    public synchronized void releaseAllLocks(TransactionId tid){
        for(PageId pid: lockMap.keySet()){
            List<PageLevelLock> locks = lockMap.get(pid);
            for(PageLevelLock l: locks){
                if(l.getTransactionId().equals(tid)){
                    locks.remove(l);
                    break;
                }
            }
        }
    }

    /**
     * return true if the transaction holds a lock
     * 
     * @param tid
     * @param pid
     */
    public synchronized Boolean holdsLock(TransactionId tid,PageId pid){
        if (lockMap.containsKey(pid)){
            List<PageLevelLock> locks = lockMap.get(pid);
            for(PageLevelLock l: locks){
                if(l.getTransactionId().equals(tid)){
                    return true;
                }
            }
        }    
        return false;
    }
}

/* page level lock */
class PageLevelLock {
    private TransactionId tid;
    private Permissions perm;

    public PageLevelLock(TransactionId tid, Permissions perm){
        this.tid =  tid;
        this.perm = perm;
    }

    public TransactionId getTransactionId(){
        return this.tid;
    }

    public Permissions getPermissions(){
        return this.perm;
    }

    public void setPermissions(Permissions perm){
        this.perm = perm;
    }
}