package com.lagou.edu.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class StreamToRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "";
        int port = 7777;
        DataStreamSource<String> data = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, String>> m_word = data.map(new MapFunction<String, Tuple2<String, String>>() {
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<String, String>("m_word", value);
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("").setPort(6379).build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<Tuple2<String, String>>(conf, new MyMapper());
        m_word.addSink(redisSink);
        env.execute();
    }

    public static class MyMapper implements RedisMapper<Tuple2<String,String>>{

        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
