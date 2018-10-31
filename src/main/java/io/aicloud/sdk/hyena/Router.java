package io.aicloud.sdk.hyena;

import com.google.protobuf.InvalidProtocolBufferException;
import io.aicloud.sdk.hyena.pb.DBState;
import io.aicloud.sdk.hyena.pb.Store;
import io.aicloud.sdk.hyena.pb.VectorDB;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Description:
 * <pre>
 * Date: 2018-10-30
 * Time: 10:32
 * </pre>
 *
 * @author fagongzi
 */
class Router {
    private Map<Long, Store> stores = new HashMap<>();
    private Map<Long, VectorDB> dbs = new HashMap<>();
    private Map<Long, Long> leaders = new HashMap<>();
    private VectorDB writableDB;
    private Transport transport;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    Router(int executors, int ioExecutors) {
        transport = new Transport(executors, ioExecutors, this::getAddress);
    }

    void onInitEvent(EventNotify message) {
        lock.writeLock().lock();
        stores.clear();
        dbs.clear();
        message.readInitEventValues(this::updateDB, this::updateStore);
        lock.writeLock().unlock();
    }

    void onDBCreatedoOrChanged(EventNotify message) {
        lock.writeLock().lock();
        try {
            updateDB(VectorDB.parseFrom(message.getValue()));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
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
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String getAddress(long id) {
        String address = null;

        lock.readLock().lock();
        Store store = stores.get(id);
        if (store != null) {
            address = store.getClientAddress();
        }
        lock.readLock().unlock();

        return address;
    }

    private void updateDB(EventNotify.ResourceValue value) {
        try {
            VectorDB db = VectorDB.parseFrom(value.getData());
            updateDB(db);

            if (value.getLeader() > 0) {
                leaders.put(db.getId(), value.getLeader());
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void updateDB(VectorDB db) {
        if (dbs.containsKey(db.getId())) {
            return;
        }

        dbs.put(db.getId(), db);
        updateWritable(db);
    }

    private void updateStore(byte[] value) {
        try {
            Store store = Store.parseFrom(value);
            updateStore(store);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    private void updateStore(Store store) {
        if (stores.containsKey(store.getId())) {
            return;
        }

        stores.put(store.getId(), store);
    }

    private void updateLeader(long id, long newLeader) {
        if (dbs.containsKey(id)) {
            return;
        }

        leaders.put(id, newLeader);
    }

    private void updateWritable(VectorDB db) {
        if (db.getState() == DBState.RWU) {
            writableDB = db;
        }
    }
}
