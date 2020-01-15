package com.wy.stream2kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @author wangyn3
 * @date 2019-12-31 13:40
 */
public class Test {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000);
//        //设置模式为：exactly_one，仅一次语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        //同一时间只允许进行一次检查点
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        Properties props = new Properties();
//        props.setProperty("bootstrap.servers","192.168.99.164:6062");
//        props.setProperty("group.id","console-consumer-45690");
//        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>("wangyntest03",new SimpleStringSchema(),props);
////        consumer.setStartFromEarliest();
//        consumer.setStartFromGroupOffsets();
//        DataStream<String> stream = env
//                .addSource(consumer);
//        stream.print();
//        //stream.map();
//        env.execute();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        System.out.println(format.parse("2020-01-05 00:00:00").getTime());
        System.out.println(format.format(date));
    }
}
