package com.wy.stream2kafka.pojo;

import lombok.Data;

/**
 * @author wangyn3
 * @date 2020-01-08 19:56
 */
@Data
public class User {



    public User(String id, String phone) {
        this.id = id;
        this.phone = phone;
    }

    public User(String id) {
        this.id = id;
        this.phone = phone;
    }

    public User(){}

    private String id;

    private String phone;
}
