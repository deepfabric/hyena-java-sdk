package io.aicloud.sdk.hyena;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.aicloud.sdk.hyena.pb.Peer;
import io.aicloud.sdk.hyena.pb.SearchRequest;
import io.aicloud.sdk.hyena.pb.Store;
import io.aicloud.sdk.hyena.pb.VectorDB;
import io.aicloud.tools.netty.ChannelAware;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Description:
 * <pre>
 * Date: 2018-10-30
 * Time: 10:32
 * </pre>
 *
 * @author fagongzi
 */
@Slf4j(topic = "hyena")
class Router {
    private long maxDB;
    private Map<Long, Store> stores = new HashMap<>();
    private Map<Long, VectorDB> dbs = new HashMap<>();
    private Map<Long, Long> leaders = new HashMap<>();
    private Map<Long, AtomicLong> ops = new HashMap<>();
    private Transport transport;
    private CountDownLatch waiter = new CountDownLatch(1);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Condition cond = lock.writeLock().newCondition();
    private BlockingQueue<WaitReq> waitQueue = new LinkedBlockingQueue<>(1024);

    Router(int executors, int ioExecutors, ChannelAware<MessageLite> channelAware) {
        transport = new Transport(executors, ioExecutors, channelAware);
        transport.start();

        startWaitQueue();
    }

    void waitForNewDB(long old) {
        try {
            lock.writeLock().lockInterruptibly();
            for (; getMaxDB() <= old; ) {
                cond.await();
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            lock.writeLock().unlock();
        }
    }

    void notifyAllWaiter() {
        try {
            lock.writeLock().lockInterruptibly();
            cond.signalAll();
        } catch (InterruptedException e) {
            // ignore
        } finally {
            lock.writeLock().unlock();
        }
    }

    void waitForComplete() throws InterruptedException {
        waiter.await();
        waiter = null;
    }

    VectorDB getDB(long id) {
        lock.readLock().lock();
        VectorDB value = dbs.get(id);
        lock.readLock().unlock();
        return value;
    }

    void doForEachDB(Consumer<VectorDB> action) {
        lock.readLock().lock();
        dbs.values().forEach(action);
        lock.readLock().unlock();
    }

    long getMaxDB() {
        lock.readLock().lock();
        long value = maxDB;
        lock.readLock().unlock();

        return value;
    }

    void onInitEvent(EventNotify message) {
        lock.writeLock().lock();
        stores.clear();
        dbs.clear();
        message.readInitEventValues(this::updateDB, this::updateStore);
        lock.writeLock().unlock();

        if (waiter != null) {
            waiter.countDown();
        }
    }

    void onDBCreatedOrChanged(EventNotify message) {
        lock.writeLock().lock();
        try {
            updateDB(VectorDB.parseFrom(message.getValue()));
        } catch (InvalidProtocolBufferException e) {
            log.error("parse failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void onDBLeaderChanged(EventNotify message) {
        EventNotify.LeaderChangeValue value = message.readLeaderChangeValue();
        lock.writeLock().lock();
        updateLeader(value.getResourceId(), value.getNewLeaderId());
        lock.writeLock().unlock();
    }

    void onStoreCreatedOrChanged(EventNotify message) {
        lock.writeLock().lock();
        try {
            updateStore(Store.parseFrom(message.getValue()));
        } catch (InvalidProtocolBufferException e) {
            log.error("parse failed", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    void startWaitQueue() {
        new Thread(() -> {
            for (; ; ) {
                try {
                    WaitReq req = waitQueue.take();
                    log.debug("offset {} start wait at db {}", req.oldReq.getOffset(), req.oldDB);
                    waitForNewDB(req.oldDB);
                    log.debug("offset {} end wait at db {}", req.oldReq.getOffset(), req.oldDB);

                    doForEachDB(db -> {
                        if (db.getId() > req.oldDB) {
                            SearchRequest target = SearchRequest.newBuilder(req.oldReq)
                                    .setDb(db.getId())
                                    .setId(ByteString.copyFrom(UUID.randomUUID().toString(), MQBasedClient.UTF8))
                                    .setLast(db.getId() == getMaxDB())
                                    .build();
                            req.action.accept(target);
                            sent(db, target);
                        }
                    });
                    req.complete.accept(null);
                } catch (Throwable e) {
                    // ignore
                }
            }
        }).start();
    }

    void sentWaitReq(long old, SearchRequest req, Consumer<SearchRequest> action, Consumer<Void> complete) {
        waitQueue.add(new WaitReq(old, req, action, complete));
    }

    void sent(VectorDB db, SearchRequest req) {
        transport.sent(selectTargetPeer(db).getStoreID(), req);
    }

    private Peer selectTargetPeer(VectorDB db) {
        lock.readLock().lock();
        AtomicLong op = ops.get(db.getId());
        Peer value = db.getPeers((int) (op.incrementAndGet() % db.getPeersCount()));
        lock.readLock().unlock();
        return value;
    }

    private void updateDB(EventNotify.ResourceValue value) {
        try {
            VectorDB db = VectorDB.parseFrom(value.getData());
            updateDB(db);

            if (value.getLeader() > 0) {
                updateLeader(db.getId(), value.getLeader());
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("parse failed", e);
        }
    }

    private void updateDB(VectorDB db) {
        if (!ops.containsKey(db.getId())) {
            ops.put(db.getId(), new AtomicLong(0));
        }

        dbs.put(db.getId(), db);

        log.debug("db-{} updated, {}",
                db.getId(),
                db.toString());

        if (db.getId() > maxDB) {
            maxDB = db.getId();
        }

        notifyAllWaiter();
    }

    private void updateStore(byte[] value) {
        try {
            Store store = Store.parseFrom(value);
            updateStore(store);
        } catch (InvalidProtocolBufferException e) {
            log.error("parse failed", e);
        }
    }

    private void updateStore(Store store) {
        stores.put(store.getId(), store);
        log.debug("store-{} updated, {}",
                store.getId(),
                store.toString());

        transport.addConnector(store);
    }

    private void updateLeader(long id, long newLeader) {
        if (!dbs.containsKey(id)) {
            log.warn("db-{} is missing", id);
            return;
        }

        leaders.put(id, newLeader);
        log.debug("db-{} leader changed to {}",
                id,
                newLeader);
    }

    private static class WaitReq {
        private long oldDB;
        private SearchRequest oldReq;
        private Consumer<SearchRequest> action;
        private Consumer<Void> complete;

        public WaitReq(long oldDB, SearchRequest oldReq, Consumer<SearchRequest> action, Consumer<Void> complete) {
            this.oldDB = oldDB;
            this.oldReq = oldReq;
            this.action = action;
            this.complete = complete;
        }
    }
}
