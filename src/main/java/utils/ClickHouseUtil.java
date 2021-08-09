package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author zhangpeng.sun
 * @ClassName: ClickHouseUtil
 * @Description TODO
 * @date 2021/8/5 9:25
 */
public class ClickHouseUtil {

    private static Connection connection;

    public static Connection getConn(String host, int port, String database) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        connection = DriverManager.getConnection(address);
        return connection;
    }

    public static Connection getConn(String host, int port) throws Exception {
        return getConn(host, port, "default");
    }

    public static Connection getConn() throws Exception {
        return getConn("node-01", 8123);
    }

    public void close() throws SQLException {
        connection.close();
    }
}
