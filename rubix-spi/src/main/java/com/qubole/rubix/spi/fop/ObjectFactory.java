package com.qubole.rubix.spi.fop;

import java.net.SocketException;

/**
 * @author Daniel
 */
public interface ObjectFactory<T> {

    T create(String host, int socketTimeout, int connectTimeout);

    void destroy(T t)
            throws SocketException;

    boolean validate(T t);

}
