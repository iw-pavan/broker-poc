package com.uniphore.common;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum MessagePriority {
    HIGH(2),
    NORMAL(1),
    LOW(0);

    private final int level;
}
