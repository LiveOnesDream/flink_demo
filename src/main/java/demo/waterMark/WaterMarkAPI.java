package demo.waterMark;

import com.flink.entity.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.Calendar;

/**
 * @author zhangpeng.sun
 * @ClassName: WaterMarkAPI
 * @Description TODO
 * @date 2021/7/27 14:12
 */
public class WaterMarkAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //事件时间，每条数据都携带时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //处理时间 数据不携带任何时间戳的信息
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //摄入时间（进入flink时间），flink会使用系统时间戳绑定到每条数据，可以防止flink内部处理数据发生乱序的情况，但无法解决到达flink之前发生的乱序问题。
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //周期生成watermark，默认200毫秒
        env.getConfig().setAutoWatermarkInterval(100L);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 777);
        SingleOutputStreamOperator<SensorReading> watermarksDatastream = dataStream.map(data -> {
            String[] fields = data.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
            //底层是周期性生成一个方法处理乱序数据，延迟1秒生成水位 同时分配水位和时间戳，括号里是等待延迟的时间
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestame() * 1000;
            }
        });

        //周期生成watermark，处理乱序数据
        dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new MyAssignerPeriodic());

    }

    //    设置水位线，周期性生成waterMark 默认200毫秒
    public static class MyAssignerPeriodic implements AssignerWithPeriodicWatermarks<SensorReading> {

        Long bound = 60 * 1000L;
        Long maxTS = Long.MAX_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //定义一个规则进行生成
            return new Watermark(maxTS - bound);
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            maxTS = sensorReading.getTimestame();
            return sensorReading.getTimestame() * 1000;
        }
    }

    //乱序生成watermark， 每来一条数据就生成一个materMark
    public static class MyAssignerPunctuated implements AssignerWithPunctuatedWatermarks<SensorReading> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(SensorReading sensorReading, long l) {
            return new Watermark(l);
        }

        @Override
        public long extractTimestamp(SensorReading sensorReading, long l) {
            return sensorReading.getTimestame();
        }
    }
}
