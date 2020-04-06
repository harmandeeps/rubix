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

/**
 * Created by sakshia on 27/9/16.
 */

import com.google.common.annotations.VisibleForTesting;
import com.qubole.rubix.spi.fop.Poolable;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.HeartbeatRequest;
import com.qubole.rubix.spi.thrift.SetCachedRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;

public class RetryingPooledBookkeeperClient
        implements Closeable
{
  private static final Log log = LogFactory.getLog(RetryingPooledBookkeeperClient.class);
  private int maxRetries;
  private TTransport transport;
  private Poolable<TTransport> transportPoolable;
  private BookKeeperService.Client bookKeeperServiceClient;
  private String host;
  private Configuration configuration;

  @VisibleForTesting
  public RetryingPooledBookkeeperClient(TTransport transport, int maxRetries)
  {
    this.bookKeeperServiceClient = new BookKeeperService.Client(new TBinaryProtocol(transport));
    this.transport = transport;
    this.maxRetries = maxRetries;
  }

  public RetryingPooledBookkeeperClient(String host, Configuration configuration)
  {
    this.host = host;
    this.configuration = configuration;
    Poolable<TTransport> obj = BookKeeperFactory.pool.borrowObject(host, configuration);
    this.bookKeeperServiceClient = new BookKeeperService.Client(new TBinaryProtocol(obj.getObject()));
    this.transport = transportPoolable.getObject();
    this.transportPoolable = obj;
    this.maxRetries = CacheConfig.getMaxRetries(configuration);
  }

  public List<BlockLocation> getCacheStatus(final CacheStatusRequest request) throws TException
  {
    return retryConnection(new Callable<List<BlockLocation>>()
    {
      @Override
      public List<BlockLocation> call()
          throws TException
      {
        return bookKeeperServiceClient.getCacheStatus(request);
      }
    });
  }

  public void setAllCached(final SetCachedRequest request) throws TException
  {
    retryConnection(new Callable<Void>()
    {
      @Override
      public Void call()
          throws Exception
      {
        bookKeeperServiceClient.setAllCached(request);
        return null;
      }
    });
  }

  public void handleHeartbeat(final HeartbeatRequest request) throws TException
  {
    retryConnection(new Callable<Void>()
    {
      @Override
      public Void call() throws Exception
      {
        bookKeeperServiceClient.handleHeartbeat(request);
        return null;
      }
    });
  }

  private <V> V retryConnection(Callable<V> callable)
      throws TException
  {
    int errors = 0;

    while (errors < maxRetries) {
      try {
        if (!transport.isOpen()) {
          transport.open();
        }
        return callable.call();
      }
      catch (Exception e) {
        log.warn("Error while connecting : ", e);
        errors++;
        Poolable<TTransport> obj = BookKeeperFactory.pool.borrowObject(host, configuration);
        this.bookKeeperServiceClient = new BookKeeperService.Client(new TBinaryProtocol(obj.getObject()));
      }
      if (transport.isOpen()) {
        transport.close();
      }
    }

    throw new TException();
  }

  @Override
  public void close()
  {
    if (transportPoolable != null && transportPoolable.getObject() != null && transportPoolable.getObject().isOpen()) {
      BookKeeperFactory.pool.returnObject(transportPoolable);
    }
  }
}
