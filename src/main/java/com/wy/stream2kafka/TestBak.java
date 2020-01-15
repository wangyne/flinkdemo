package com.wy.stream2kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * @author wangyn3
 * @date 2019-12-30 11:24
 */
public class TestBak {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String outputPath=params.get("output");
        String topic=params.get("topic");
        System.out.println(outputPath);
        System.out.println(topic);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义，默认既为EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
//        env.setStateBackend(new FsStateBackend("file:///D:\\downloads\\chromdownloads"));
        env.setStateBackend((StateBackend)new FsStateBackend("file:///D:\\downloads\\flinkcheckpoint"));

        //设置kafka消费参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","192.168.99.164:6062");
        props.setProperty("group.id","console-consumer-45690");
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.164:6062");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group1");
        //kafka分区自动发现周期
        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer011<>("wangyntest03", new JSONKeyValueDeserializationSchema(true), props);
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(topic,new SimpleStringSchema(), props);
//        kafkaConsumer011.setCommitOffsetsOnCheckpoints(true);
//        kafkaConsumer011.setStartFromEarliest();
        System.out.println(kafkaConsumer011);
        //加入kafka数据源
        DataStream<String> kafkaStreamSource = env.addSource(kafkaConsumer011);
//        kafkaStreamSource.writeAsText("file:///D:\\downloads\\file\\aa", FileSystem.WriteMode.NO_OVERWRITE);

//        kafkaStreamSource.writeAsText("file:/data/flinkdata/output/aa", FileSystem.WriteMode.NO_OVERWRITE);
//        kafkaStreamSource.writeAsText(outputPath, FileSystem.WriteMode.NO_OVERWRITE).name(topic+"..."+outputPath);
//        System.out.println(kafkaStreamSource);
//        DataStreamSource<String> streamSource = env.socketTextStream("localhost",9999,"\n");
        //数据传输到下游
//        streamSource.writeUsingOutputFormat(new MySqlTwoPhaseCommitSink());
//        kafkaStreamSource.addSink(new MysqlSink()).name("MysqlSink").setParallelism(1);
        kafkaStreamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink").setParallelism(1);
        //触发执行
        env.execute(StreamDemoKafka2Mysql.class.getName());
//          env.execute();
    }
}


