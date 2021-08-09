package demo.sink;

import com.flink.entity.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhangpeng.sun
 * @date S
 * Copyright @2021 Tima Networks Inc. All Rights Reserved.
 */
public class MySQLSinkAPI {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("E:\\tima_workspace\\flink_demo\\src\\main\\resources\\WordCount.txt");
        DataStream<SensorReading> lineStream = dataStream.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        lineStream.addSink(new MyJDBCSink());
    }

    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc://mysql:localhost:3360/test", "root", "12345");
            insertStmt = connection.prepareStatement("insert intto sensor_temp (id,temp) value (?,?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp=? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1, value.getTt());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();

            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTt());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }
}
