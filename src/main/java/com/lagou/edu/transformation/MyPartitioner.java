package com.lagou.edu.transformation;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartitioner implements Partitioner<Long> {

    public int partition(Long key, int numPartitions) {
        System.out.println("一共有" + numPartitions + "个分区");
        if(key % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
