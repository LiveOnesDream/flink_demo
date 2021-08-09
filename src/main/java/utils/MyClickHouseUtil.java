package utils;

import com.flink.entity.UserInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author zhangpeng.sun
 * @ClassName: MyClickHouseUtil
 * @Description TODO
 * @date 2021/8/5 9:28
 */
public class MyClickHouseUtil extends RichSinkFunction<UserInfo> {
    Connection connection = null;
    String sql;


    public MyClickHouseUtil(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickHouseUtil.getConn("121.40.30.72", 8123, "bigdata");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(UserInfo info, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, info.getId());
        preparedStatement.setInt(2, info.getProvince_id());
        preparedStatement.setInt(3, info.getCity_id());
        preparedStatement.setString(4, info.getBehavior_type());
        preparedStatement.setInt(5, info.getBehavior_id());
        preparedStatement.setInt(6, info.getEntry_type());
        preparedStatement.setInt(7, info.getUser_id());
        preparedStatement.setInt(8, info.getOn_off_line());
        preparedStatement.setInt(9, info.getSource());
        preparedStatement.setInt(10, info.getIs_index_a());
        preparedStatement.setInt(11, info.getIs_index_i());
        preparedStatement.setInt(12, info.getIs_index_p());
        preparedStatement.setInt(13, info.getIs_index_l());
        preparedStatement.setString(14, info.getOther_data());
        preparedStatement.setTimestamp(15, info.getCreate_time());
        preparedStatement.setTimestamp(16, info.getCreate_time());
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}
