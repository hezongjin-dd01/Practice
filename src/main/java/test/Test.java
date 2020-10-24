package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class Test {

    public static void main(String[] args) {
        String url = "jdbc:clickhouse://10.112.0.40:8123/ODS_LOCAL?useUnicode=true&characterEncoding=UTF-8";
        String sql = "select  DISTINCT cid from ODS_LOCAL.kafka_ce10_paid_order";

        List<String> list = executeSqls(url, sql);
//        System.out.println(list.size());

        list.forEach(new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);
            }
        });
    }

    public static List<String> executeSqls(String url, String sql) {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        List<String> list = new ArrayList<>();
        try {

            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(url, "default", "Yxqk5NTn");
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String cid = resultSet.getString("cid");
                list.add(cid);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }
}
