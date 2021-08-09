package demo.sink;

import com.flink.entity.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import utils.ExecutionEnvUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static utils.KafkaConfigUtil.buildKafkaProps;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class KafkaAndEsSinkAPI {
    public static void main(String[] args) throws Exception {
        //获取application.properties 文件
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        //构建flink启动参数配置
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        //构建kafka参数
        Properties props = buildKafkaProps(parameterTool);
        //kafka topic list
        List<String> topics = Arrays.asList(parameterTool.get("metrics.topic"), parameterTool.get("logs.topic"));
        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(topics, new SimpleStringSchema(), props);
        env.addSource(consumer).print();

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "121.40.30.72:9092,120.55.88.211:9092,121.41.82.205:9092");
//        properties.setProperty("zookeeper.connect","121.40.30.72:2181,120.55.88.211:2181,121.41.82.205:2181");
//        properties.setProperty("group.id", "consumer-group1");
//        properties.setProperty("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer",
//                "org.apache.kafka. common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
//        DataStream<String> dataStream = env.readTextFile("E:\\WorkSpace\\flink_demo\\src\\main\\resources\\WordCount.txt");
//        dataStream.addSink(new FlinkKafkaProducer011<String>("121.40.30.72:9092,120.55.88.211:9092,121.41.82.205:9092", "sensor", new SimpleStringSchema()));

//        --------------------------es------------------------------
//        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
//        httpHosts.add(new HttpHost("localhost", 9200));

//        DataStream<SensorReading> lineStream = dataStream.map(line -> {
//            String[] fields = line.split(" ");
//            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//        });
//        lineStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());
        env.execute();
    }

    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void process(SensorReading sensorReading, RuntimeContext ctx, RequestIndexer index) {
            HashMap<String, String> datasource = new HashMap<String, String>();
            datasource.put("id", sensorReading.getId());
            datasource.put("temp", sensorReading.getTimestame().toString());
            datasource.put("tt", sensorReading.getTt().toString());
//           创建请求，作为向es发起写入命令
            IndexRequest indexRequest = Requests.indexRequest()
                    .index("sensor")
                    .type("readingdata")
                    .source(datasource);
//            用index发送请求
            index.add(indexRequest);
        }
    }
}
