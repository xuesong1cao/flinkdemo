package com.lagou.edu.demo;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.nio.cs.StreamEncoder;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class WaterDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> data = env.socketTextStream("teacher2", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> maped = data.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], Long.valueOf(split[1]));
            }
        });
//        WatermarkStrategy.<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
//                .withIdleness(Duration.ofMinutes(1));
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = maped.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxTimestamp = 0l;
            final Long maxOutOfOrderness = 10000l;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                long timestamp = element.f1;
//                System.out.println("timestamp:" + timestamp + "..." );
//                System.out.println("..." +  sdf.format(timestamp));

                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("key:" + element.f0
                                + "...eventtime:[" + element.f1 + "|" + sdf.format(element.f1)
                        /*+ "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp)*/
                        /*+ "],watermark:[" + getCurrentWatermark().getTimestamp() + "| " + sdf.format(getCurrentWatermark().getTimestamp() + "]")*/);
                System.out.println("currentMaxTimestamp" + currentMaxTimestamp + "..." + sdf.format(currentMaxTimestamp));
                System.out.println("watermark:" + getCurrentWatermark().getTimestamp() + "..." + sdf.format(getCurrentWatermark().getTimestamp()));
                return timestamp;
            }
        });
        SingleOutputStreamOperator<String> res = watermarks.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3))).apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out) throws Exception {
                String key = tuple.toString();
                ArrayList<Long> list = new ArrayList<>();
                Iterator<Tuple2<String, Long>> it = input.iterator();
                while (it.hasNext()) {
                    Tuple2<String, Long> next = it.next();
                    list.add(next.f1);
                }
                Collections.sort(list);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String result = key + "," + list.size() + "," + sdf.format(list.get(0)) + "," + sdf.format(list.get(list.size() - 1)) + "," + sdf.format(window.getStart()) + "," + sdf.format(window.getEnd());
                out.collect(result);
            }
        });
        res.print();
        env.execute();
    }
}
