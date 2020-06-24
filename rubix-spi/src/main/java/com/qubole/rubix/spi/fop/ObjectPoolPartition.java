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

import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


import static com.google.common.base.Preconditions.checkState;

/**
 * @author Daniel
 */
public class ObjectPoolPartition<T>
{
  private static final Log log = LogFactory.getLog(ObjectPoolPartition.class);

  private final ObjectPool<T> pool;
  private final PoolConfig config;
  private final BlockingQueue<Poolable<T>> objectQueue;
  private final ObjectFactory<T> objectFactory;
  private int totalCount;
  private final String host;
  private final int socketTimeout;
  private final int connectTimeout;
  private final String name;
  private AtomicInteger alive = new AtomicInteger();
  private AtomicInteger created = new AtomicInteger();
  private AtomicInteger destroyed = new AtomicInteger();
  private AtomicInteger used = new AtomicInteger();

  public ObjectPoolPartition(ObjectPool<T> pool, PoolConfig config,
          ObjectFactory<T> objectFactory, BlockingQueue<Poolable<T>> queue, String host, String name)
  {
    this.pool = pool;
    this.config = config;
    this.objectFactory = objectFactory;
    this.objectQueue = queue;
    this.host = host;
    this.name = name;
    this.socketTimeout = config.getSocketTimeoutMilliseconds();
    this.connectTimeout = config.getConnectTimeoutMilliseconds();
    this.totalCount = 0;
    for (int i = 0; i < config.getMinSize(); i++) {
      T object = objectFactory.create(host, socketTimeout, connectTimeout);
      if (object != null) {
        objectQueue.add(new Poolable<>(object, pool, host, totalCount));
        totalCount++;
        alive.incrementAndGet();
        created.incrementAndGet();
        if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
          log.info(String.format("aaa: ObjectPoolPartition: host: %s, Alive: %s, Created: %s, Destroyed: %s", host, alive.get(), created.get(), destroyed.get()));
        }
      }
    }
  }

  public void returnObject(Poolable<T> object)
  {
    used.decrementAndGet();
    boolean isValid = objectFactory.validate(object.getObject());
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(host + " aaa: opp: returnObject: object: " + object.getObject() + " isvlaid: " + isValid);
    }
    if (!isValid) {
      if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
        log.info(String.format(host + " aaa: opp: returnObject: %s : Invalid object for host %s removing %s ", this.name, object.getHost(), object));
      }
      decreaseObject(object);
      // Compensate for the removed object. Needed to prevent endless wait when in parallel a borrowObject is called
      increaseObjects(1, false);
      return;
    }

    int beforeSize = objectQueue.size();
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(String.format(host + " aaa: opp: returnObject: %s : Returning object %s to queue of host %s. Before Queue size: %d", this.name, object, object.getHost(), beforeSize));
    }
    boolean isOffered = objectQueue.offer(object);
    int afterSize = objectQueue.size();
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(String.format(host + " aaa: opp: returnObject: %s : Returning object %s to queue of host %s. After Queue size: %d", this.name, object, object.getHost(), afterSize));
    }
    if (!objectQueue.contains(object)) {
      if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
        log.info(host + " aaa: opp: error: dones not contain: " + object);
      }
    }
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(host + " aaa: opp: returnObject: object: " + object.getObject() + " isOffered: " + isOffered);
    }
    if (!isOffered) {
      if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
        log.warn(this.name + " : Created more objects than configured. Created=" + totalCount + " QueueSize=" + objectQueue.size());
      }
      decreaseObject(object);
    }
  }

  public Poolable<T> getObject(boolean blocking)
  {
    if (objectQueue.size() == 0) {
      // increase objects and return one, it will return null if pool reaches max size or if object creation fails
      Poolable<T> object = increaseObjects(this.config.getDelta(), true);

      if (object != null) {
        used.incrementAndGet();
        if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
          log.info(String.format(host + " aaa: opp: host: %s getObject: %s : get object %s from queue of host %s. After Queue size: %d", host, this.name, object, object.getHost(), objectQueue.size()));
        }
        return object;
      }

      if (totalCount == 0) {
        // Could not create objects, this is mostly due to connection timeouts hence no point blocking as there is not other producer of sockets
        throw new RuntimeException("Could not add connections to pool");
      }
      // else wait for a connection to get free
    }

    Poolable<T> freeObject;
    try {
      if (blocking) {
        freeObject = objectQueue.take();
      }
      else {
        freeObject = objectQueue.poll(config.getMaxWaitMilliseconds(), TimeUnit.MILLISECONDS);
        if (freeObject == null) {
          throw new RuntimeException("Cannot get a free object from the pool");
        }
        if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
          log.info(String.format(host + " aaa: opp: host: %s getObject: %s : get object %s from queue of host %s. After Queue size: %d", host, this.name, freeObject, freeObject.getHost(), objectQueue.size()));
        }
      }
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e); // will never happen
    }

    freeObject.setLastAccessTs(System.currentTimeMillis());
    used.incrementAndGet();
    return freeObject;
  }

  private synchronized Poolable<T> increaseObjects(int delta, boolean returnObject)
  {
    int oldCount = totalCount;
    if (delta + totalCount > config.getMaxSize()) {
      delta = config.getMaxSize() - totalCount;
    }

    Poolable<T> objectToReturn = null;
    try {
      for (int i = 0; i < delta; i++) {
        T object = objectFactory.create(host, socketTimeout, connectTimeout);
        if (object != null) {
          // Do not put the first object on queue
          // it will be returned to the caller to ensure it's request is satisfied first if object is requested
          Poolable<T> poolable = new Poolable<>(object, pool, host, totalCount);
          if (objectToReturn == null && returnObject) {
            objectToReturn = poolable;
          }
          else {
            objectQueue.put(poolable);
          }
          totalCount++;
          alive.incrementAndGet();
          created.incrementAndGet();
          if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
            log.info(String.format("aaa: increaseObjects: Host: %s, Alive: %s, Created: %s, Destroyed: %s", host, alive.get(), created.get(), destroyed.get()));
          }
        }
      }

      if (delta > 0 && (totalCount - oldCount) == 0) {
        log.warn(String.format("Could not increase pool size. Pool state: totalCount=%d queueSize=%d delta=%d", totalCount, objectQueue.size(), delta));
      }
      else {
        log.debug(String.format("%s : Increased pool size by %d, to new size: %d, current queue size: %d, delta: %d",
                this.name, totalCount - oldCount, totalCount, objectQueue.size(), delta));
      }
    }
    catch (Exception e) {
      log.warn(String.format("Unable to increase pool size. Pool state: totalCount=%d queueSize=%d delta=%d", totalCount, objectQueue.size(), delta), e);
      // objectToReturn is not on the queue hence untracked, clean it up before forwarding exception
      if (objectToReturn != null) {
        objectFactory.destroy(objectToReturn.getObject());
        objectToReturn.destroy();
      }
      throw new RuntimeException(e);
    }

    return objectToReturn;
  }

  public boolean decreaseObject(Poolable<T> obj)
  {
    checkState(obj.getHost() != null, "Invalid object");
    checkState(obj.getHost().equals(this.host),
            "Call to free object of wrong partition, current partition=%s requested partition = %s",
            this.host, obj.getHost());
    objectRemoved();
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(this.name + " : Decreasing pool size for " + this.host + " , object: " + obj);
    }
    objectFactory.destroy(obj.getObject());
    obj.destroy();
    alive.decrementAndGet();
    destroyed.incrementAndGet();
    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      log.info(String.format("aaa: decreaseObject: Hist: %s, Alive: %s, Created: %s, Destroyed: %s", host, alive.get(), created.get(), destroyed.get()));
    }
    return true;
  }

  private synchronized void objectRemoved()
  {
    totalCount--;
  }

  public synchronized int getTotalCount()
  {
    return totalCount;
  }

  // set the scavenge interval carefully
  public synchronized void scavenge() throws InterruptedException
  {
    int delta = this.totalCount - config.getMinSize();
    if (delta <= 0) {
      log.debug(this.name + " : Scavenge for delta <= 0, Skipping !!!");
      return;
    }
    int removed = 0;
    long now = System.currentTimeMillis();
    Poolable<T> obj;
    obj = objectQueue.poll();
    while (delta-- > 0 && obj != null) {
      // performance trade off: delta always decrease even if the queue is empty,
      // so it could take several intervals to shrink the pool to the configured min value.
      log.debug(String.format("%s : obj=%s, now-last=%s, max idle=%s", this.name, obj, now - obj.getLastAccessTs(),
              config.getMaxIdleMilliseconds()));
      if (now - obj.getLastAccessTs() > config.getMaxIdleMilliseconds() &&
              ThreadLocalRandom.current().nextDouble(1) < config.getScavengeRatio()) {
        log.debug(this.name + " : Scavenger removing object: " + obj);
        decreaseObject(obj); // shrink the pool size if the object reaches max idle time
        removed++;
      }
      else {
        objectQueue.put(obj); //put it back
      }
      obj = objectQueue.poll();
    }
    if (removed > 0) {
      log.info("aaa: Scavenger: " + this.name + " : Host: " + this.host + " : " + removed + " objects were scavenged.");
    }
  }

  public synchronized boolean validate()
  {
    boolean isValid = ( alive.get() == ( created.get() - destroyed.get() ) );
    try {
      String ipAddress = this.host.equalsIgnoreCase("localhost") ? "127.0.0.1" : this.host;
      Process p = Runtime.getRuntime().exec(new String[]{"sh","-c", String.format("netstat -ant | grep %s:%s | grep ESTABLISHED | wc -l", ipAddress, config.getPort())},
              null, null);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String s = stdInput.readLine();
      int aliveConnection = Integer.parseInt(s.trim());
      if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
        log.info(String.format("aaa: Validate: %s: Host: %s: NetstatAlive: %s Alive: %s, Created: %s, Destroyed: %s, In Queue: %s, Used: %s",
                name, host, aliveConnection, alive.get(), created.get(), destroyed.get(), objectQueue.size(), used.get()));
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    if (this.host.equalsIgnoreCase("localhost")) {
      return isValid;
    }

    if (!host.equalsIgnoreCase("localhost") && !name.equalsIgnoreCase("lds-pool")) {
      StringBuilder sb = new StringBuilder();
      for (Poolable<T> poolable : objectQueue) {
        sb.append(String.format(", Obj: %s Number: %s", poolable.getObject(), poolable.getNumber()));
      }
      log.info(String.format("aaa: %s: host: %s opp: pool connections: %s, pool size: %s", name, host, sb.toString(), objectQueue.size()));
    }
    return isValid;
  }

  public synchronized int shutdown()
  {
    int removed = 0;
    while (this.totalCount > 0) {
      Poolable<T> obj = objectQueue.poll();
      if (obj != null) {
        decreaseObject(obj);
        removed++;
      }
    }
    return removed;
  }

  public String getHost()
  {
    return host;
  }
}
