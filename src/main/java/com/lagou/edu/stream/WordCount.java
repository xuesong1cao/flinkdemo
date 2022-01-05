package com.lagou.edu.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 	Socket模拟实时发送单词，使用Flink实时接收数据，对指定时间窗口内（如5s）的数据进行聚合统计，每隔1s汇总计算一次，并且把时间窗口内计算结果打印出来。

 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 监听的ip和端口号，以main参数形式传入，约定第一个参数为ip，第二个参数为端口
        String ip = "192.168.56.1";
        int port = 7777;
        // 获取Flink流执行环境
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取socket输入数据
        DataStreamSource<String> textStream = streamExecutionEnvironment.socketTextStream(ip, port, "\n");

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] splits = s.split("\\s");
                for (String word : splits) {
                    collector.collect(Tuple2.of(word, 1l));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> word = tuple2SingleOutputStreamOperator.keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);
        // 打印数据
        word.print();
        // 触发任务执行
        streamExecutionEnvironment.execute("wordcount stream process");

    }
}
