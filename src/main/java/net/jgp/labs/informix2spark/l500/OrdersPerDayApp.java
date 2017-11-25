package net.jgp.labs.informix2spark.l500;

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
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.Set.Set2;

public class OrdersPerDayApp {

  public static void main(String[] args) {
    OrdersPerDayApp app = new OrdersPerDayApp();
    app.start();
  }

  private void start() {
    SparkSession spark;

    // @formatter:off
    spark = SparkSession
        .builder()
        .appName("Sales per day")
        .master("local")
        .getOrCreate();
    // @formatter:on

    // List of all tables we want to work with
    List<String> tables = new ArrayList<>();
    tables.add("orders");
    tables.add("items");

    // Specific Informix dialect
    JdbcDialect dialect = new InformixJdbcDialect();
    JdbcDialects.registerDialect(dialect);

    // Let's connect to the database
    Config config = ConfigManager.getConfig(K.INFORMIX);

    // Let's build our datalake
    Map<String, Dataset<Row>> datalake = new HashMap<>();
    for (String table : tables) {
      System.out.print("Loading table [" + table + "] ... ");
      // @formatter:off
      Dataset<Row> df = spark.read()
          .format("jdbc")
          .option("url", config.getJdbcUrl())
          .option("dbtable", table)
          .option("user", config.getUser())
          .option("password", config.getPassword())
          .option("driver", config.getDriver())
          .load();
      // @formatter:on

      datalake.put(table, df);
      System.out.println("done");
    }

    System.out.println("We have loaded " + datalake.size()
        + " table(s) in our data lake");

    // Let's look at the content
    Dataset<Row> ordersDf = datalake.get("orders");
    Dataset<Row> itemsDf = datalake.get("items");

    // @formatter:off
    Dataset<Row> allDf = ordersDf
        .join(
            itemsDf, 
            ordersDf.col("order_num").equalTo(itemsDf.col("order_num")), 
            "full_outer")
        .drop(ordersDf.col("customer_num"))
        .drop(itemsDf.col("order_num"))
        .groupBy(ordersDf.col("order_date"))
        .sum("total_price")
        .orderBy(ordersDf.col("order_date"));
    // @formatter:on
    allDf.cache();
    allDf.printSchema();
    allDf.show(50);
  }
}
