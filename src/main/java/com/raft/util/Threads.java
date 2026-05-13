package com.raft.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class Threads {

    private Threads() {}

    public static ScheduledExecutorService singleThreadScheduledExecutor(String namePrefix) {
        return Executors.newSingleThreadScheduledExecutor(threadFactory(namePrefix));
    }

    public static ThreadFactory threadFactory(String namePrefix) {
        return new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, namePrefix + "-" + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
    }
}
