package demo.source;

import com.flink.entity.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class MySourceAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> sensorReadingDataStreamSource = env.addSource(new MySensorSource());

        SplitStream<SensorReading> split = sensorReadingDataStreamSource.split(new OutputSelector<SensorReading>() {
            public Iterable<String> select(SensorReading value) {
                return (value.getTt() > 30) ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        DataStream<SensorReading> all = split.select("high", "low");

        high.print();
        low.print();
        all.print();


        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        private boolean running = true;

        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            HashMap<String, Double> hashMap = new HashMap<String, Double>(1024);
            int count = 10;
            for (int i = 0; i < count; i++) {
                hashMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sensorId : hashMap.keySet()) {
                    Double newTemp = hashMap.get(sensorId) + random.nextGaussian();
                    hashMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(10000);
                cancel();
            }
        }

        public void cancel() {
            running = false;
        }
    }
}
