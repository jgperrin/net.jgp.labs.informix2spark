package net.jgp.labs.informix2spark.l500;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.weekofyear;

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

public class OrdersPerWeekApp {

  public static void main(String[] args) {
    OrdersPerWeekApp app = new OrdersPerWeekApp();
    app.start();
  }

  private void start() {
    SparkSession spark;

    // @formatter:off
    spark = SparkSession
        .builder()
        .appName("Sales per week")
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


    Dataset<Row> allDf = ordersDf
        .join(
            itemsDf,
            ordersDf.col("order_num").equalTo(itemsDf.col("order_num")),
            "full_outer")
        .drop(ordersDf.col("customer_num"))
        .drop(itemsDf.col("order_num"))
        .withColumn("order_week", lit(weekofyear(ordersDf.col("order_date"))));
    allDf = allDf
        .groupBy(allDf.col("order_week"))
        .sum("total_price")
        .orderBy(allDf.col("order_week"));

    allDf.cache();
    allDf.printSchema();
    allDf.show(50);
  }
}
