package com.lagou.edu.demo;


import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author nandy
 * @create 2021/3/4 9:50
 */
@Slf4j
public class KafkaWriter {
    //本地的kafka机器列表
    public static final String BROKER_LIST = "192.168.56.107:9092,192.168.56.107:9093,192.168.56.107:9094";
    //kafka的topic
    public static final String TOPIC_PERSON = "student";
    //key序列化的方式，采用字符串的形式
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    //value的序列化的方式
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void writeToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            //构建Person对象，在name为hqs后边加个随机数
            int randomInt = RandomUtils.nextInt(1, 100000);
            Student student = new Student();
            student.setName("nandy" + randomInt);
            student.setAge(randomInt);
            student.setCreateDate(LocalDateTime.now().toString());
            //转换成JSON
            String personJson = JSON.toJSONString(student);

            //包装成kafka发送的记录
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_PERSON, null,
                    null, personJson);
            //发送到缓存
            producer.send(record);
            log.info("向kafka发送数据:" + personJson);
            //立即发送
            producer.flush();
        }
    }

    public static void main(String[] args) {
        int count = 0;
        while (count < 20) {
            try {
                //每三秒写一条数据
                TimeUnit.SECONDS.sleep(3);
                writeToKafka();
                count++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

