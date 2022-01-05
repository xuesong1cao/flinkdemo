package com.lagou.edu.transformation;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySource implements SourceFunction<Long> {

    private long count = 1;
    private boolean isRunning = true;

    public void run(SourceContext<Long> ctx) throws Exception {
        while(isRunning) {
            ctx.collect(count);
            count ++;
            Thread.sleep(1000);
        }
    }

    public void cancel() {
        isRunning = false;
    }
}
