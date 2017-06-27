package net.jgp.labs.informix2spark.l100;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

import net.jgp.labs.informix2spark.utils.Config;
import net.jgp.labs.informix2spark.utils.ConfigManager;
import net.jgp.labs.informix2spark.utils.InformixJdbcDialect;
import net.jgp.labs.informix2spark.utils.K;

public class DatabaseLoader {

  public static void main(String[] args) {
    DatabaseLoader app = new DatabaseLoader();
    app.start();
  }

  private void start() {
    // @formatter:off
    SparkSession spark = SparkSession
        .builder()
        .appName("Stores Data")
        .master("local")
        .getOrCreate();
    // @formatter:on

    // Let's connect to the database
    Config config = ConfigManager.getConfig(K.INFORMIX);
    Connection connection = config.getConnection();
    if (connection == null) {
      return;
    }

    // List of all tables
    List<String> tables = getTables(connection);
    if (tables.isEmpty()) {
      return;
    }

    // Specific Informix dialect
    JdbcDialect dialect = new InformixJdbcDialect();
    JdbcDialects.registerDialect(dialect);

    Map<String, Dataset<Row>> database = new HashMap<>();
    for (String table : tables) {
      System.out.print("Loading table [" + table
          + "] ... ");
      // @formatter:off
      Dataset<Row> df = spark
  	.read()
  	.format("jdbc")
  	.option("url", config.getJdbcUrl())
  	.option("dbtable", table)
  	.option("user", config.getUser())
  	.option("password", config.getPassword())
  	.option("driver", config.getDriver())
  	.load();
      // @formatter:on
      database.put(table, df);
      System.out.println("done");
    }

    System.out.println("We have " + database.size()
        + " table(s) in our database");
    Dataset<Row> df = database.get("state");
    df.cache();
    df.printSchema();
    System.out.println("Number of rows in state: " + df
        .count());
    df.show(5);
  }

  /**
   * 
   * @param connection
   * @param database
   * @return
   */
  private List<String> getTables(Connection connection) {
    List<String> tables = new ArrayList<>();
    DatabaseMetaData md;
    try {
      md = connection.getMetaData();
    } catch (SQLException e) {
      return tables;
    }

    ResultSet rs;
    try {
      rs = md.getTables(null, null, "%", null);
    } catch (SQLException e) {
      return tables;
    }

    try {
      while (rs.next()) {
        String tableName = rs.getString(3);
        String tableType = rs.getString(4).toLowerCase();
        System.out.print("Table [" + tableName + "] ... ");
        if (tableType.compareTo("table") == 0 || tableType
            .compareTo("view") == 0) {
          tables.add(tableName);
          System.out.println("is in (" + tableType + ").");
        } else {
          System.out.println("is out (" + tableType + ").");
        }
      }
    } catch (SQLException e) {
      return tables;
    }

    return tables;
  }
}
