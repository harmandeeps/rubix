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
package com.qubole.rubix.spi.fop;

import com.qubole.rubix.spi.CacheConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.net.Socket;

public class SocketObjectFactory
    implements ObjectFactory<SocketObjectFactory.BookkeeperTSocket>
{
  private static final Log log = LogFactory.getLog(SocketObjectFactory.class.getName());
  private static final String BKS_POOL = "bks-pool";
  private String host;
  private final int port;

  public SocketObjectFactory(int port)
  {
    this.port = port;
  }

  @Override
  public BookkeeperTSocket create(String host, int socketTimeout, int connectTimeout)
  {
    this.host = host;
    if (!host.equalsIgnoreCase("localhost")) {
      log.info(BKS_POOL + " : Opening connection to host: " + host);
    }
    BookkeeperTSocket socket = null;
    try {
      socket = new BookkeeperTSocket(host, port, socketTimeout, connectTimeout);
      socket.open();
    }
    catch (TTransportException e) {
      socket = null;
      log.warn("Unable to open connection to host " + host, e);
    }
    return socket;
  }

  @Override
  public void destroy(BookkeeperTSocket o)
  {
    // clean up and release resources
    if (!host.equalsIgnoreCase("localhost")) {
      log.info(BKS_POOL + " : Destroy socket channel: " + o + " for host: " + host);
    }
    o.close();
  }

  @Override
  public boolean validate(BookkeeperTSocket o)
  {
    boolean isClosed = (o != null && !o.isOpen());

    // Saw that transport.close did not close socket, explicitly closing socket
    if (isClosed && o != null) {
      try {
        o.getSocket().close();
      }
      catch (IOException e) {
        // Let os time it out
      }
    }

    if (!host.equalsIgnoreCase("localhost")) {
      log.info(BKS_POOL + " : Validate socket channel: for host: " + host + " socket channel: " + o + " isvalid: " + !isClosed);
    }
    return !isClosed;
  }

  public static ObjectPool<BookkeeperTSocket> createSocketObjectPool(Configuration conf, String host, int port)
  {
    log.info(BKS_POOL + " : Creating socket object pool");
    PoolConfig poolConfig = new PoolConfig();
    poolConfig.setMaxSize(CacheConfig.getTranportPoolMaxSize(conf));
    poolConfig.setMinSize(CacheConfig.getTransportPoolMinSize(conf));
    poolConfig.setDelta(CacheConfig.getTransportPoolDeltaSize(conf));
    poolConfig.setMaxWaitMilliseconds(CacheConfig.getTransportPoolMaxWait(conf));
    poolConfig.setScavengeIntervalMilliseconds(CacheConfig.getScavengeInterval(conf));
    //poolConfig.setConnectTimeoutMilliseconds(CacheConfig.getServerSocketTimeout(conf));
    poolConfig.setConnectTimeoutMilliseconds(CacheConfig.getServerConnectTimeout(conf));
    poolConfig.setSocketTimeoutMilliseconds(CacheConfig.getServerSocketTimeout(conf));
    poolConfig.setPort(port);

    ObjectFactory<BookkeeperTSocket> factory = new SocketObjectFactory(port);
    ObjectPool<BookkeeperTSocket> pool = new ObjectPool(poolConfig, factory, BKS_POOL);
    pool.registerHost(host);
    return pool;
  }

  public static class BookkeeperTSocket extends TSocket {

    public BookkeeperTSocket(Socket socket)
            throws TTransportException
    {
      super(socket);
    }

    public BookkeeperTSocket(String host, int port)
    {
      super(host, port);
    }

    public BookkeeperTSocket(String host, int port, int timeout)
    {
      super(host, port, timeout);
    }

    public BookkeeperTSocket(String host, int port, int socketTimeout, int connectTimeout)
    {
      super(host, port, socketTimeout, connectTimeout);
    }

    /**
     * Closes the socket.
     */
    public void close() {
      log.info("bts: closing tsocket: " + this);
      super.close();
    }
  }
}
