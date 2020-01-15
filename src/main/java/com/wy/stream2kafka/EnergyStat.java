package com.wy.stream2kafka;

import com.wy.stream2kafka.pojo.EnergyDetail;
import com.wy.stream2kafka.util.FastJsonUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * @author wangyn3
 * @date 2020-01-10 15:28
 */
public class EnergyStat {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend((StateBackend) new FsStateBackend("file:///D:\\downloads\\flinkcheckpoint"));




        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.99.164:6062");
        props.setProperty("group.id", "console-consumer-36701");
        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>("wangyntest04", new SimpleStringSchema(), props);

        DataStream<String> kafkaStreamSource = env.addSource(kafkaConsumer011);
//{type:"create"}


        DataStream energyDetailDataStream =kafkaStreamSource.map(x->{
            return FastJsonUtils.toBean(x,EnergyDetail.class) ;
        }).map(x->{
            return new Tuple2<String,Integer>(x.getUserId(), x.getEnergyValue());
        }).returns(Types.TUPLE(Types.STRING,Types.INT))
//                .filter(x->{
//            return "1".equals(x.getBizType())&&x.getGmtCreated().after(format.parse("2020-01-05 00:00:00"));
//        })
                .keyBy(0).sum(1)
//                .map(x->{
////           return new Tuple2<String,Integer>(x.getUserId(),x.getEnergyValue());
////                })
 ;
        energyDetailDataStream.writeAsText("D:\\downloads\\file\\bb.txt",FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("EnergyStat");
    }
}
