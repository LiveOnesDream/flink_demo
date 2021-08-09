package demo.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class KafkaSourceAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "121.40.30.72:9092,120.55.88.211:9092,121.41.82.205:9092");
        properties.setProperty("zookeeper.connect","121.40.30.72:2181,120.55.88.211:2181,121.41.82.205:2181");

        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka. common.serialization.StringDeserializer");
//      latest 最近 earliest最开始
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(
                new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        dataStreamSource.print();
        env.execute();

    }
}
