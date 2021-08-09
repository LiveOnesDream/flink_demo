package clickhouse;

import com.flink.entity.UserInfo;
import mingdu.AnalogData;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import utils.MyClickHouseUtil;
import utils.MySQLUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author zhangpeng.sun
 * @ClassName: ClickHouseSink
 * @Description TODO
 * @date 2021/8/5 9:34
 */
public class ClickHouseSink {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStreamSource<UserInfo> userInfoDataStream = env.addSource(new AnalogData.SourceFromMySQL());
        userInfoDataStream.print();
        String sql = "INSERT INTO bigdata.md_user_info_alalysis (id, province_id, city_id, behavior_type, behavior_id, entry_type, user_id, on_off_line, `source`, is_index_a, is_index_i, is_index_p, is_index_l, other_data, request_time, create_time)  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
        MyClickHouseUtil clickHouseSink = new MyClickHouseUtil(sql);
        userInfoDataStream.addSink(clickHouseSink);
        userInfoDataStream.print();

        env.execute("mysql 2 clickHouse");
    }

    public static class SourceFromMySQL extends RichSourceFunction<UserInfo> {
        PreparedStatement ps;
        private Connection connection;

        /**
         * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                    "jdbc:mysql://118.31.17.237:3306/one_data?useUnicode=true&characterEncoding=UTF-8",
                    "bearer",
                    "eLRF8Iev5RQi");
            String sql = "select * from md_user_info_alalysis;";
            ps = this.connection.prepareStatement(sql);
        }

        /**
         * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) { //关闭连接和释放资源
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        /**
         * DataStream 调用一次 run() 方法用来获取数据
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<UserInfo> ctx) throws Exception {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                UserInfo userInfo = new UserInfo(
                        resultSet.getInt("id"),
                        resultSet.getInt("province_id"),
                        resultSet.getInt("city_id"),
                        resultSet.getString("behavior_type"),
                        resultSet.getInt("behavior_id"),
                        resultSet.getInt("entry_type"),
                        resultSet.getInt("user_id"),
                        resultSet.getInt("on_off_line"),
                        resultSet.getInt("source"),
                        resultSet.getInt("is_index_a"),
                        resultSet.getInt("is_index_i"),
                        resultSet.getInt("is_index_p"),
                        resultSet.getInt("is_index_l"),
                        resultSet.getString("other_data"),
                        resultSet.getTimestamp("create_time"),
                        resultSet.getTimestamp("request_time")
                );
                ctx.collect(userInfo);
            }
        }

        @Override
        public void cancel() {
        }
    }
}
