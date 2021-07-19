package demo.source;

import com.flink.entity.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class CollectionSourceAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStreamSource = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor-1", 19327327987L, 23.5),
                        new SensorReading("sensor-2", 98312687326L, 32.5)
                ));
        dataStreamSource.print();
        env.execute();
    }
}
