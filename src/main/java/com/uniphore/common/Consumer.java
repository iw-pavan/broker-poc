package com.uniphore.common;

public interface Consumer {

    void consume(Queue queue, java.util.function.Consumer<byte[]> consumer);

}
