package demo.window;

import com.flink.entity.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhangpeng.sun
 * @date s
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class WindowAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("E:\\tima_workspace\\flink_demo\\src\\main\\resources\\WordCount.txt");
        DataStream<SensorReading> lineStream = dataStream.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        DataStream<Integer> result = lineStream.keyBy(0)
                //滚动计时窗口
                .timeWindow(Time.minutes(15))
//        .window(TumblingProcessingTimeWindows.of(Time.minutes(15))); 滚动窗口
//        .countWindow(2); 滚动计数窗口
//        .countWindow(10,2);滑动计数窗口
//        .window(EventTimeSessionWindows.withGap(Time.minutes(10))); 会话窗口

                //增量聚合
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    //创建累加器
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    //累加
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        //全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> apply = lineStream.keyBy(0)
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> output) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        output.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });
        //开窗计数函数
        lineStream.keyBy(0)
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp()).print();

        //延迟过长的数据
        OutputTag<SensorReading> outputTag = new OutputTag<>("late");
        //延迟数据
        SingleOutputStreamOperator<SensorReading> sumStream = lineStream.keyBy(0)
                .timeWindow(Time.seconds(15))
                //允许延迟1分钟的数据
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum(1);
        //获取延迟过长的数据
        sumStream.getSideOutput(outputTag).print();

        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<Double, Integer>(accumulator.f0 + value.getTt(), accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
