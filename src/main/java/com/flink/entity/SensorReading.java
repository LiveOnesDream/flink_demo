package com.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhangpeng.sun
 * @date s
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    private String id;
    private Long timestame;
    private Double tt;
}
