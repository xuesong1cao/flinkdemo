package com.lagou.edu.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SelfPartitionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Long> data = env.addSource(new MySource());

        SingleOutputStreamOperator<Tuple1<Long>> mapded = data.map(new MapFunction<Long, Tuple1<Long>>() {
            public Tuple1<Long> map(Long value) throws Exception {
                System.out.println("map1当前多线程ID" + Thread.currentThread().getId() + ".value:" + value);
                return new Tuple1<Long>(value);
            }
        });
        DataStream<Tuple1<Long>> partitioned = mapded.partitionCustom(new MyPartitioner(), 0);
        SingleOutputStreamOperator<Long> result = partitioned.map(new MapFunction<Tuple1<Long>, Long>() {
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("map2当前多线程ID" + Thread.currentThread().getId() + ".value:" + value);
                return value.getField(0);
            }
        });
        result.print().setParallelism(1);
        env.execute("selfpartitionDemo");
    }
}
