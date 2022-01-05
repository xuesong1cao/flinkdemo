package com.lagou.edu.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "";
        int port = 7777;
        DataStreamSource<String> data = env.socketTextStream(hostname, port);
        //创建窗口（TimeWindow)
        SingleOutputStreamOperator<Tuple1<String>> tupled = data.map(new MapFunction<String, Tuple1<String>>() {
            public Tuple1<String> map(String s) throws Exception {
                return new Tuple1<String>(s);
            }
        });
        KeyedStream<Tuple1<String>, Tuple> keyed = tupled.keyBy(0);
        WindowedStream<Tuple1<String>, Tuple, TimeWindow> sessionWindow = keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));
        SingleOutputStreamOperator<String> result = sessionWindow.apply(new WindowFunction<Tuple1<String>, String, Tuple, TimeWindow>() {
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple1<String>> iterable, Collector<String> collector) throws Exception {
                for (Tuple1<String> t : iterable) {
                    collector.collect(t.getField(0).toString());
                }

            }
        });
        result.print();
        env.execute();
    }
}
