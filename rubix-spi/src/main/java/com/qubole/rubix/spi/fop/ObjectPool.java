/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 * <p>
 * <p>
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY  Qubole Inc UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
 * OF https://github.com/DanielYWoo/fast-object-pool.
 */
package com.qubole.rubix.spi.fop;

import org.apache.commons.logging.LogFactory;

import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Daniel
 */
public class ObjectPool<T>
{
  private static org.apache.commons.logging.Log log = LogFactory.getLog(ObjectPool.class.getName());

  private final PoolConfig config;
  private final ObjectFactory<T> factory;
  private final ObjectPoolPartition<T>[] partitions;
  private Scavenger scavenger;
  private volatile boolean shuttingDown;

  public ObjectPool(PoolConfig poolConfig, ObjectFactory<T> objectFactory)
  {
    this.config = poolConfig;
    this.factory = objectFactory;
    this.partitions = new ObjectPoolPartition[config.getPartitionSize()];
    if (config.getScavengeIntervalMilliseconds() > 0) {
      this.scavenger = new Scavenger();
      this.scavenger.start();
    }
  }

  public void registerHost(String host, int socketTimeout, int connectTimeout, int index)
  {
    partitions[index] = new ObjectPoolPartition<>(this, index, config, factory, createBlockingQueue(config), host, socketTimeout, connectTimeout);
  }

  protected BlockingQueue<Poolable<T>> createBlockingQueue(PoolConfig poolConfig)
  {
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
        return result;
      }
      else {
        this.partitions[result.getPartition()].decreaseObject(result);
      }
    }
    throw new RuntimeException("Cannot find a valid object");
  }

  private Poolable<T> getObject(boolean blocking, int partitionNumber)
  {
    if (shuttingDown) {
      throw new IllegalStateException("Your pool is shutting down");
    }
    int partition = partitionNumber % this.config.getPartitionSize();
    ObjectPoolPartition<T> subPool = this.partitions[partition];
    log.info("aaa: getObject: partition: " + partition);
    if (subPool == null) {
      log.info("aaa: subPool is null for partition: " + partition);
    }
    log.info("aaa: before: subPool object queuer size after get obect: " + subPool.getObjectQueue().size());

    Poolable<T> freeObject;
    if (subPool.getObjectQueue().size() == 0) {
      // increase objects and return one, it will return null if reach max size
      subPool.increaseObjects(this.config.getDelta());
      try {
        if (blocking) {
          log.info("aaa: in blocking mode, waiting.....");
          freeObject = subPool.getObjectQueue().take();
        }
        else {
          freeObject = subPool.getObjectQueue().poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
          if (freeObject == null) {
            throw new RuntimeException("Cannot get a free object from the pool");
          }
        }
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e); // will never happen
      }
    }
    else {
      freeObject = subPool.getObjectQueue().poll();
    }
    log.info("aaa: after: subPool object queuer size after get obect: " + subPool.getObjectQueue().size() + " obj: " + freeObject.getObject());
    freeObject.setLastAccessTs(System.currentTimeMillis());
    return freeObject;
  }

  public void returnObject(Poolable<T> obj)
  {
    ObjectPoolPartition<T> subPool = this.partitions[obj.getPartition()];
    try {
      log.info("aaa: before: return object: queue size: " + subPool.getObjectQueue().size() + ", partition id: " + obj.getPartition() + " obj: " + obj.getObject());
      subPool.getObjectQueue().put(obj);
      log.info("aaa: after: return object: queue size:" + subPool.getObjectQueue().size() + ", partition id:" + obj.getPartition() + " obj: " + obj.getObject());
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e); // impossible for now, unless there is a bug, e,g. borrow once but return twice.
    }
  }

  public int getSize()
  {
    int size = 0;
    for (ObjectPoolPartition<T> subPool : partitions) {
      size += subPool.getTotalCount();
    }
    return size;
  }

  public synchronized int shutdown()
          throws InterruptedException
  {
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

  private class Scavenger
          extends Thread
  {
    @Override
    public void run()
    {
      int partition = 0;
      while (!ObjectPool.this.shuttingDown) {
        try {
          Thread.sleep(config.getScavengeIntervalMilliseconds());
          partition = ++partition % config.getPartitionSize();
          Log.debug("scavenge sub pool ", partition);
          partitions[partition].scavenge();
        }
        catch (InterruptedException ignored) {
        }
        catch (SocketException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
