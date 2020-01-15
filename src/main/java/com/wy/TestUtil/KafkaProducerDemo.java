package com.wy.TestUtil;

import com.alibaba.fastjson.JSON;
import com.wy.stream2kafka.pojo.EnergyDetail;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.Properties;

/**
 * @author wangyn3
 * @date 2020-01-10 14:54
 */
public class KafkaProducerDemo {
    private static String jdbcUrl = "jdbc:mysql://192.168.1.230:3307/weiyi_operate?useSSL=false";
    private static String username = "search_rw";
    private static String password = "search_rw_pwd";
    private static String driverName = "com.mysql.jdbc.Driver";



    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        PreparedStatement ps = conn.prepareStatement("select * from weiyi_operate.energy_detail");
        ResultSet rs = ps.executeQuery();
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.99.164:6062");
        // 指定key.serializer属性。必填，无默认值。被发送到broker端的任何消息的格式都必须是字节数组。
        // 因此消息的各个组件都必须首先做序列化，然后才能发送到broker。该参数就是为消息的key做序列化只用的。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 指定value.serializer属性。必填，无默认值。和key.serializer类似。此被用来对消息体即消息value部分做序列化。
        // 将消息value部分转换成字节数组。
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        while (rs.next()){
            EnergyDetail energyDetail=new EnergyDetail();
            energyDetail.setId(rs.getString("id"));
            energyDetail.setUserId(rs.getString("user_id"));
            energyDetail.setBizType(rs.getString("biz_type"));
            energyDetail.setEnergyValue(rs.getInt("energy_value"));
            energyDetail.setGmtCreated(rs.getTimestamp("gmt_created"));

            String json =JSON.toJSONString(energyDetail);
            System.out.println(json);
            producer.send(new ProducerRecord<String,String>("wangyntest04",json));
        }
    }
}
