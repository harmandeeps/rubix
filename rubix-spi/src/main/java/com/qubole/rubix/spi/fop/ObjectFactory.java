package com.qubole.rubix.spi.fop;

/**
 * @author Daniel
 */
public interface ObjectFactory<T> {

    T create();

    void destroy(T t);

    boolean validate(T t);

}
