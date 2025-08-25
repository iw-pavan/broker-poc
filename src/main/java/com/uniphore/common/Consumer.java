package com.uniphore.common;

import java.io.Closeable;

public interface Consumer extends Closeable {

    void consume(Queue queue, java.util.function.Consumer<byte[]> consumer);

}
