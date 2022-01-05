package com.lagou.edu.demo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
@Slf4j
public class FlinkReadDbWriterDb {

    public static void main(String[] args)   {
        // 构建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.107:9092");
        props.put("zookeeper.connect", "192.168.56.107:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(
                new FlinkKafkaConsumer010<String>(
                        //这个 kafka topic 需要和上面的工具类的 topic 一致
                        "student",
                        new SimpleStringSchema(),
                        props))
                //单线程打印，控制台不乱序，不影响结果
                .setParallelism(1);
        //从kafka里读取数据，转换成Person对象
        DataStream<Student> dataStream = dataStreamSource.map(string -> JSON.parseObject(string, Student.class));

        //收集5秒钟的总数
       dataStream.timeWindowAll(Time.seconds(5L)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Student> iterable, Collector<List<Student>> collector) throws Exception {
                List<Student> students = Lists.newArrayList(iterable);
                if(CollectionUtils.isNotEmpty(students)){
                    log.info("5秒的总共收到的条数：" + students.size());
                    collector.collect(students);
                }
            }
            //sink 到数据库
        }).addSink(new Flink2JdbcWriter());
//        .addSink(new Flink2JdbcWriter())

        try {
            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

