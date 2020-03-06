package com.qubole.rubix.spi.fop;

import org.apache.commons.logging.LogFactory;
import org.apache.thrift.transport.TSocket;

import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Daniel
 */
public class ObjectPool<T> {

    private static org.apache.commons.logging.Log log = LogFactory.getLog(ObjectPool.class.getName());

    private final PoolConfig config;
    private final ObjectFactory<T> factory;
    private final ObjectPoolPartition<T>[] partitions;
    private Scavenger scavenger;
    private volatile boolean shuttingDown;

    public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory) {
        this.config = poolConfig;
        this.factory = objectFactory;
        this.partitions = new ObjectPoolPartition[config.getPartitionSize()];
        if (config.getScavengeIntervalMilliseconds() > 0) {
            this.scavenger = new Scavenger();
            this.scavenger.start();
        }
    }

    public void registerHost(String host, int socketTimeout, int connectTimeout, int index) {
            partitions[index] = new ObjectPoolPartition<>(this, index, config, factory, createBlockingQueue(config), host, socketTimeout, connectTimeout);
    }

    protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig) {
        return new ArrayBlockingQueue<>(poolConfig.getMaxSize());
    }

    public Poolable<T> borrowObject(int partitionNumber)
            throws SocketException
    {
        return borrowObject(true, partitionNumber);
    }

    public Poolable<T> borrowObject(boolean blocking, int partitionNumber)
            throws SocketException
    {
        log.info("aaa: borrowObject: Partition: " + partitionNumber);
        for (int i = 0; i < 3; i++) { // try at most three times
            Poolable<T> result = getObject(blocking, partitionNumber);
            if (factory.validate(result.getObject())) {
                ((TSocket)result.getObject()).getSocket().setSoTimeout(600000);
                return result;
            } else {
                this.partitions[result.getPartition()].decreaseObject(result);
            }
        }
        throw new RuntimeException("Cannot find a valid object");
    }

    private Poolable<T> getObject(boolean blocking, int partitionNumber) {
        if (shuttingDown) {
            throw new IllegalStateException("Your pool is shutting down");
        }
        int partition = partitionNumber % this.config.getPartitionSize();
        ObjectPoolPartition<T> subPool = this.partitions[partition];
        Poolable<T> freeObject = subPool.getObjectQueue().poll();
        if (freeObject == null) {
            // increase objects and return one, it will return null if reach max size
            subPool.increaseObjects(1);
            try {
                if (blocking) {
                    freeObject = subPool.getObjectQueue().take();
                } else {
                    freeObject = subPool.getObjectQueue().poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
                    if (freeObject == null) {
                        throw new RuntimeException("Cannot get a free object from the pool");
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e); // will never happen
            }
        }
        freeObject.setLastAccessTs(System.currentTimeMillis());
        return freeObject;
    }

    public void returnObject(Poolable<T> obj) {
        ObjectPoolPartition<T> subPool = this.partitions[obj.getPartition()];
        try {
            subPool.getObjectQueue().put(obj);
            if (Log.isDebug())
                Log.debug("return object: queue size:", subPool.getObjectQueue().size(),
                    ", partition id:", obj.getPartition());
        } catch (InterruptedException e) {
            throw new RuntimeException(e); // impossible for now, unless there is a bug, e,g. borrow once but return twice.
        }
    }

    public int getSize() {
        int size = 0;
        for (ObjectPoolPartition<T> subPool : partitions) {
            size += subPool.getTotalCount();
        }
        return size;
    }

    public synchronized int shutdown() throws InterruptedException {
        shuttingDown = true;
        int removed = 0;
        if (scavenger != null) {
            scavenger.interrupt();
            scavenger.join();
        }
        for (ObjectPoolPartition<T> partition : partitions) {
            removed += partition.shutdown();
        }
        return removed;
    }

    private class Scavenger extends Thread {

        @Override
        public void run() {
            int partition = 0;
            while (!ObjectPool.this.shuttingDown) {
                try {
                    Thread.sleep(config.getScavengeIntervalMilliseconds());
                    partition = ++partition % config.getPartitionSize();
                    Log.debug("scavenge sub pool ",  partition);
                    partitions[partition].scavenge();
                } catch (InterruptedException ignored) {
                }
                catch (SocketException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
