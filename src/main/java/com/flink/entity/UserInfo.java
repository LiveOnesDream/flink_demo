package com.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @author zhangpeng.sun
 * @ClassName: UserInfo
 * @Description TODO
 * @date 2021/8/4 16:11
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserInfo {

    private int id;
    private int province_id;
    private int city_id;
    private String behavior_type;
    private int behavior_id;
    private int entry_type;
    private int user_id;
    private int on_off_line;
    private int source;
    private int is_index_a;
    private int is_index_i;
    private int is_index_p;
    private int is_index_l;
    private String other_data;
    private Timestamp create_time;
    private Timestamp request_time;
}
