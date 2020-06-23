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
 */
package com.qubole.rubix.spi;

import com.qubole.rubix.spi.fop.ObjectPool;
import com.qubole.rubix.spi.fop.Poolable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.Closeable;
import java.util.Random;
import java.util.concurrent.Callable;

public abstract class RetryingPooledThriftClient
    implements Closeable
{
  private static final Log log = LogFactory.getLog(RetryingPooledThriftClient.class);

  private final int maxRetries;
  private final Configuration conf;
  private final String host;

  private Poolable<TTransport> transportPoolable;
  protected TServiceClient client;

  public RetryingPooledThriftClient(int maxRetries, Configuration conf, String host, Poolable<TTransport> transportPoolable)
  {
    this.maxRetries = maxRetries;
    this.conf = conf;
    this.host = host;
    this.transportPoolable = transportPoolable;
  }

  private void updateClient(Poolable<TTransport> transportPoolable)
  {
    this.client = setupClient(transportPoolable);
  }

  public abstract TServiceClient setupClient(Poolable<TTransport> transportPoolable);

  protected <V> V retryConnection(Callable<V> callable)
      throws TException
  {
    int errors = 0;

    if (client == null) {
      updateClient(transportPoolable);
    }

    while (errors < maxRetries) {

      //RandomClose.closeChannelRandomly(transportPoolable.getObject(), 100);

      try {
        return callable.call();
      }
      catch (Exception e) {
        log.warn("Error while connecting : " + e);

        if (e instanceof TException)
        {
          log.warn("rptc: closing socket so that it wont be added back to the pool TTransportException: " + e);
          transportPoolable.getObject().close();
          transportPoolable.getPool().returnObject(transportPoolable);
        }

        errors++;
        // We dont want to keep the transport around in case of exception to prevent reading old results in transport reuse
        // Get a reference to objectPool before it is destroyed in returnObject
        ObjectPool<TTransport> objectPool = transportPoolable.getPool();
        if (transportPoolable.getObject().isOpen()) {
          log.warn("rptc: Transport is open lets return it back to the pool");
          // Close connection and submit back so that ObjectPool to handle decommissioning
          client.getInputProtocol().getTransport().close();
          transportPoolable.getPool().returnObject(transportPoolable);
        }

        // unset transportPoolable so that close() doesnt return it again to pool if borrowObject hits an exception
        transportPoolable = null;
        transportPoolable = objectPool.borrowObject(host, conf);
        updateClient(transportPoolable);
      }
    }

    throw new TException();
  }

  @Override
  public void close()
  {
    log.info("aaa: rptc: in close: " + transportPoolable);
    if (transportPoolable != null) {
      log.info("aaa: rptc: in close, transportPoolable is not null: " + transportPoolable.getObject());
      transportPoolable.getPool().returnObject(transportPoolable);
    }
  }

  public Poolable<TTransport> getTransportPoolable()
  {
    return transportPoolable;
  }

  static class RandomClose
  {
    public static void closeChannelRandomly(TTransport tTransport, int max)
    {
      Random rand = new Random();
      int random = rand.nextInt(max);
      log.info("aaa: random: " + random);
      if (random == 0) {
        log.info("aaa: randomClose of RetryingPooledThriftClient: " + tTransport);
        tTransport.close();
      }
    }
  }
}
