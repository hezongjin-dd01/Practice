package kafka

import java.sql.DriverManager

object Demo3 {

  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://mini1:9803", "hive", "123456")
    val pstmt = conn.prepareStatement("select * from test.person")
    val rs = pstmt.executeQuery()
    while (rs.next()) {
      println("key:" + rs.getString("key") +
        " , value:" + rs.getString("value"))
    }
    rs.close()
    pstmt.close()
    conn.close()
  }
}
