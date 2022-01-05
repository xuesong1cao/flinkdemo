package com.lagou.edu.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        //获取数据源
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

        WindowedStream<Tuple1<String>, Tuple, TimeWindow> timeWindow = keyed.timeWindow(Time.seconds(10),Time.seconds(5));
        WindowedStream<Tuple1<String>, Tuple, GlobalWindow> countWindow = keyed.countWindow(5l,3);
        SingleOutputStreamOperator<String> result = countWindow.apply(new WindowFunction<Tuple1<String>, String, Tuple, GlobalWindow>() {
            public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple1<String>> iterable, Collector<String> collector) throws Exception {
                for (Tuple1<String> t : iterable) {
                    String s = t.getField(0).toString();
                    collector.collect(s);
                }
            }
        });

//        SingleOutputStreamOperator<String> result = timeWindow.apply(new WindowFunction<Tuple1<String>, String, Tuple, TimeWindow>() {
//            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple1<String>> iterable, Collector<String> collector) throws Exception {
//                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//                long start = timeWindow.getStart();
//                long end = timeWindow.getEnd();
//
//                String startTime = simpleDateFormat.format(start);
//                String endTime = simpleDateFormat.format(end);
//                String key = tuple.getField(0).toString();
//
//                System.out.println("key:" + key + "...startTime:" + startTime + "...endTime:" + endTime);
//                for (Tuple1<String> t : iterable) {
//                    String s = t.getField(0).toString();
//                    collector.collect(s);
//                }
//            }
//        });
        result.print();
        env.execute();
    }
}
