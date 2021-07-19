package demo.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.collection.immutable.Stream;

/**
 * @author zhangpeng.sun
 * @date s
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\tima_workspace\\flink_demo\\src\\main\\resources\\WordCount.txt");

        SingleOutputStreamOperator<Object> sum = dataStreamSource.flatMap(new FlatMapFunction<String, Object>() {
            public void flatMap(String s, Collector<Object> collector) throws Exception {
                String[] s1 = s.split(" ");
            }
        }).keyBy(0).sum(1);
        sum.print();
        env.execute();

    }
}
