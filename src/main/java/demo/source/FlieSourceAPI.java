package demo.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class FlieSourceAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\tima_workspace\\flink_demo\\src\\main\\resources\\WordCount.txt");
        dataStreamSource.print();

        env.execute();
    }
}
