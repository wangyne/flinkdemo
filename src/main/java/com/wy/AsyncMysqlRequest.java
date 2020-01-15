package com.wy;

import com.wy.stream2kafka.pojo.User;
import com.wy.stream2kafka.util.AsyncDatabaseRequest;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;


import com.alibaba.fastjson.JSON;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author wangyn3
 * @date 2020-01-08 20:03
 */
public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath = params.get("output");
        String topic = params.get("topic");
        System.out.println(outputPath);
        System.out.println(topic);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend((StateBackend) new FsStateBackend("file:///D:\\downloads\\flinkcheckpoint"));

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(10))
                .cleanupFullSnapshot()
                .build();
        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
        stateDescriptor.enableTimeToLive(ttlConfig);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.99.164:6062");
        props.setProperty("group.id", "console-consumer-36701");
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), props);

        DataStream<String> kafkaStreamSource = env.addSource(kafkaConsumer011);
        DataStream<User> userDataStream = kafkaStreamSource.map(value->{
           return new User(value);
        }).setParallelism(4);

        DataStream<User> async=AsyncDataStream.orderedWait(userDataStream,new AsyncDatabaseRequest(),1000, TimeUnit.MICROSECONDS, 100);
        async.map(user -> {
            return JSON.toJSON(user).toString();
        }).print();

        env.execute("asyncForMysql");
    }
}
