package com.wy.stream2kafka.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.sql.Timestamp;

/**
 * @author wangyn3
 * @date 2020-01-10 14:56
 */

@Data
public class EnergyDetail {

    @JSONField(name="id")
    private String id;

    @JSONField(name="extend_code")
    private String extendCode;

    @JSONField(name="user_id")
    private String userId;

    @JSONField(name="state")
    private String state;

    @JSONField(name="task_type")
    private String taskType;

    @JSONField(name="energy_value")
    private int energyValue;

    @JSONField(name="remark")
    private String remark;

    @JSONField(name="biz_type")
    private String bizType;

    @JSONField(name="gmt_created")
    private Timestamp gmtCreated;

    @JSONField(name="gmt_modified")
    private Timestamp gmtModified;
}
