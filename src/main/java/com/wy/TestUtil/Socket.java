package com.wy.TestUtil;

import com.wy.stream2kafka.pojo.EnergyDetail;
import com.wy.stream2kafka.util.FastJsonUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wangyn3
 * @date 2020-01-11 09:41
 */
public class Socket {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> socket= env.socketTextStream("localhost",9999);
        socket.map(x->{
            EnergyDetail energyDetail= FastJsonUtils.toBean(x,EnergyDetail.class) ;
            return new Tuple2<String,Integer>((String)energyDetail.getUserId(),(Integer) energyDetail.getEnergyValue());

        }).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(0).sum(1).print();

        env.execute("socket");
    }
}
