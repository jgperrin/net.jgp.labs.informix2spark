package net.jgp.labs.informix2spark.l200;

import static org.apache.spark.sql.functions.datediff;

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

public class TimeToShipApp {

  public static void main(String[] args) {
    TimeToShipApp app = new TimeToShipApp();
    app.start();
  }

  private void start() {
    // @formatter:off
     SparkSession spark = SparkSession
        .builder()
        .appName("Time to Ship")
        .master("local")
        .getOrCreate();
    // @formatter:on

    // Specific Informix dialect
    JdbcDialect dialect = new InformixJdbcDialect();
    JdbcDialects.registerDialect(dialect);

    // Configuration info for the database
    Config config = ConfigManager.getConfig(K.INFORMIX);

    // List of all tables we want to work with
    List<String> tables = new ArrayList<>();
    tables.add("orders");

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
      System.out.println("done.");
    }

    System.out.println("We have loaded " + datalake.size()
        + " table(s) in our data lake.");

    Dataset<Row> ordersDf = datalake.get("orders");
    // @formatter:off
    ordersDf = ordersDf.withColumn(
        "time_to_ship", 
        datediff(ordersDf.col("ship_date"), ordersDf.col("order_date")));
    // @formatter:on

    ordersDf.printSchema();
    ordersDf.show(10);
    System.out.println("We have " + ordersDf.count() + " orders");

    Dataset<Row> ordersDf2 = ordersDf.filter("time_to_ship IS NOT NULL");
    ordersDf2.printSchema();
    ordersDf2.show(5);
    System.out.println("We have " + ordersDf2.count() + " delivered orders");

  }
}
