package org.puppy.hadoop.hive;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 简单的使用jdbc查询
 * @author puppy(陶江航 taojianghangdsjb @ smegz.cn)
 * @since 2021/3/12 16:59
 */
public class HiveServer2App {

    public static void main(String[] args) throws SQLException {

        DruidDataSource source = new DruidDataSource();
        source.setUrl("jdbc:hive2://hadoop000:10000");
        source.setDbType("hive");
        source.setUsername("hadoop");
        source.setPassword("123456789");
        DruidPooledConnection connection = source.getConnection();
        System.out.println("获取到连接:" + connection);
        PreparedStatement statement = connection.prepareStatement("select * from test_db.u_data");
        try {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String phone = resultSet.getString("phone");
                System.out.println("手机号码为:" + phone);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            connection.close();
            statement.close();
        }
    }
}
