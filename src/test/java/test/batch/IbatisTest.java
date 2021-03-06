package test.batch;

import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ibatis.common.jdbc.ScriptRunner;
import com.ibatis.common.resources.Resources;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapClientBuilder;

public class IbatisTest {

  private static SqlMapClient sqlmapClient;

  @BeforeClass
  public static void setUp() throws Exception {

    // Create the SQLMapClient
    Reader reader = Resources.getResourceAsReader("test/batch/ibatis-config.xml");
    sqlmapClient = SqlMapClientBuilder.buildSqlMapClient(reader);

    sqlmapClient.startTransaction();
    Connection conn = sqlmapClient.getCurrentConnection();
    reader = Resources.getResourceAsReader("test/batch/CreateDB.sql");
    ScriptRunner runner = new ScriptRunner(conn, true, true);
    runner.setLogWriter(null);
    runner.runScript(reader);
    reader.close();
    sqlmapClient.endTransaction();

  }

  @Test
  public void insertOneMillionUsers() throws SQLException {
    sqlmapClient.startTransaction();
    try {
      User user = new User();
      user.setId(1);
      user.setName("User");
      sqlmapClient.startBatch();
      for (int i = 0; i < 1000000; i++) {
        sqlmapClient.insert("insertUser", user);
      }
      sqlmapClient.executeBatch();
    } finally {
      sqlmapClient.endTransaction();
    }
  }

}
