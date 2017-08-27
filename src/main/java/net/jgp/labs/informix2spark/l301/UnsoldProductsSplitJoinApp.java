package net.jgp.labs.informix2spark.l301;

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

public class UnsoldProductsSplitJoinApp {

  public static void main(String[] args) {
    UnsoldProductsSplitJoinApp app = new UnsoldProductsSplitJoinApp();
    app.start();
  }

  private void start() {
    SparkSession spark;

    // @formatter:off
    spark = SparkSession
        .builder()
        .appName("Stores Join Analysis")
        .master("local")
        .getOrCreate();
    // @formatter:on

    // List of all tables we want to work with
    List<String> tables = new ArrayList<>();
    tables.add("customer");
    tables.add("orders");
    tables.add("items");
    tables.add("stock");

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
    for (String table : tables) {
      Dataset<Row> df = datalake.get(table);
      System.out.println("Number of rows in " + table + ": " + df.count());
      df.show(10);
      df.printSchema();
    }

    Dataset<Row> ordersDf = datalake.get("orders");
    Dataset<Row> customerDf = datalake.get("customer");
    Dataset<Row> itemsDf = datalake.get("items");
    Dataset<Row> stockDf = datalake.get("stock");

    System.out.println("----------------------------------------");
    System.out.println("First join");
    // @formatter:off
    Dataset<Row> allDf = customerDf.join(
        ordersDf, 
        customerDf.col("customer_num").equalTo(ordersDf.col("customer_num")), 
        "full_outer");
    // @formatter:on
    allDf.printSchema();

    allDf = allDf.drop(customerDf.col("fname"));
    allDf = allDf.drop(customerDf.col("lname"));
    allDf = allDf.drop(customerDf.col("address1"));
    allDf = allDf.drop(customerDf.col("address2"));
    allDf = allDf.drop(customerDf.col("city"));
    allDf = allDf.drop(customerDf.col("state"));
    allDf = allDf.drop(customerDf.col("zipcode"));
    allDf = allDf.drop(customerDf.col("phone"));
    allDf = allDf.drop(customerDf.col("fname"));
    allDf = allDf.drop(customerDf.col("lname"));
    allDf = allDf.drop(customerDf.col("address1"));
    allDf = allDf.drop(customerDf.col("address2"));
    allDf = allDf.drop(customerDf.col("city"));
    allDf = allDf.drop(customerDf.col("state"));
    allDf = allDf.drop(customerDf.col("zipcode"));
    allDf = allDf.drop(customerDf.col("phone"));
    allDf = allDf.drop(ordersDf.col("backlog"));
    allDf = allDf.drop(ordersDf.col("ship_instruct"));
    allDf = allDf.drop(ordersDf.col("ship_date"));
    allDf = allDf.drop(ordersDf.col("ship_weight"));
    allDf = allDf.drop(ordersDf.col("ship_charge"));
    allDf.show(50);

    System.out.println("----------------------------------------");
    System.out.println("First column drop");
    allDf = allDf.drop(ordersDf.col("customer_num"));
    allDf.printSchema();
    allDf.show();

    System.out.println("----------------------------------------");
    System.out.println("Second join + Removing extra column");
    // @formatter:off
    allDf = allDf.join(
        itemsDf, 
        ordersDf.col("order_num").equalTo(itemsDf.col("order_num")), 
        "full_outer");
    // @formatter:on
    allDf = allDf.drop(itemsDf.col("order_num"));

    System.out.println("----------------------------------------");
    System.out.println("Third join + Removing extra columns");
    Seq<String> stockColumns = new Set2<>("stock_num", "manu_code").toSeq();
    allDf = allDf.join(stockDf, stockColumns, "full_outer");
    allDf = allDf.drop(stockDf.col("stock_num"));
    allDf = allDf.drop(stockDf.col("manu_code"));

    allDf.cache();
    allDf.printSchema();
    allDf.show();

    List<String> columnsToDrop = new ArrayList<>();
    columnsToDrop.add("zipcode");
    columnsToDrop.add("phone");
    columnsToDrop.add("customer_num");
    columnsToDrop.add("fname");
    columnsToDrop.add("lname");
    columnsToDrop.add("company");
    columnsToDrop.add("address1");
    columnsToDrop.add("address2");
    columnsToDrop.add("city");
    columnsToDrop.add("state");
    columnsToDrop.add("order_num");
    columnsToDrop.add("order_date");
    columnsToDrop.add("customer_num");
    columnsToDrop.add("ship_instruct");
    columnsToDrop.add("backlog");
    columnsToDrop.add("po_num");
    columnsToDrop.add("ship_date");
    columnsToDrop.add("ship_weight");
    columnsToDrop.add("ship_charge");
    columnsToDrop.add("paid_date");
    columnsToDrop.add("time_to_ship");
    columnsToDrop.add("item_num");
    columnsToDrop.add("quantity");
    columnsToDrop.add("total_price");

    Dataset<Row> unsoldProductsDf = allDf.filter("order_num IS NULL").filter(
        "description IS NOT NULL").drop(JavaConversions.asScalaBuffer(columnsToDrop));
    unsoldProductsDf.cache();
    System.out.println("We have " + unsoldProductsDf.count()
        + " unsold references in our warehouse, time to do something!");
    unsoldProductsDf.show();

  }
}
