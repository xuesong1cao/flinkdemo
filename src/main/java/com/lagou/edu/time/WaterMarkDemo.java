package com.lagou.edu.time;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
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

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        DataStreamSource<String> data = env.socketTextStream("192.168.56.108", 7777);
        SingleOutputStreamOperator<Tuple2<String, Long>> maped = data.map(new MapFunction<String, Tuple2<String, Long>>() {

            public Tuple2<String, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<String, Long>(split[0], Long.valueOf(split[1]));
            }
        });
        //做出水印watermark
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarks = maped.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
            Long currentMaxStamp = 0l;
            Long maxOrderOut = 2000l;

            @Nullable
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxStamp - maxOrderOut);
            }

            public long extractTimestamp(Tuple2<String, Long> element, long l) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                Long f1 = element.f1;
                currentMaxStamp = Math.max(f1,currentMaxStamp);
                System.out.println("EventTime:" + f1 + "...f1.format:" + sdf.format(f1) + "...watermark:" + getCurrentWatermark().getTimestamp() + "...watermark.format:" + sdf.format(getCurrentWatermark().getTimestamp()));
                return f1;
            }
        });
        SingleOutputStreamOperator<String> result = watermarks.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(2))).apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                long start = timeWindow.getStart();
                long end = timeWindow.getEnd();
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String startTime = simpleDateFormat.format(start);
                String endTime = simpleDateFormat.format(end);

                Iterator<Tuple2<String, Long>> iterator = iterable.iterator();
                ArrayList<Long> list = new ArrayList<Long>();

                while (iterator.hasNext()) {
                    Tuple2<String, Long> next = iterator.next();
                    String key = next.f0;
                    Long f1 = next.f1;
                    list.add(f1);
                }
                Collections.sort(list);
                String firstTime = simpleDateFormat.format(list.get(0));
                String lastTime = simpleDateFormat.format(list.get(list.size() - 1));

                collector.collect("startTime:" + startTime + "...endTime:" + endTime + "...firstTime:" + firstTime + "...lastTime:" + lastTime);
            }
        });
//      result.addSink()
        result.print();
        env.execute();
    }
}
