package net.jgp.labs.informix2spark.l420;

import static org.apache.spark.sql.functions.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class SalesTarget {

  SparkSession spark;

  public static void main(String[] args) {
    SalesTarget app = new SalesTarget();
    app.start();
  }

  private void start() {
    init();

    Dataset<Row> householdDf = getHouseholdDataframe();
    Dataset<Row> populationDf = getPopulationDataframe();
    Dataset<Row> indexDf = joinHouseholdPopulation(
        householdDf, populationDf);
    Dataset<Row> salesDf = getSalesData();

    Dataset<Row> salesIndexDf = salesDf.join(indexDf,
        salesDf.col("zipcode").equalTo(indexDf.col(
            "zipcode")), "left").drop(indexDf.col(
                "zipcode")).orderBy(col("revenue").desc());
    salesIndexDf.show();
  }

  private void init() {
    // @formatter:off
    this.spark = SparkSession
        .builder()
        .appName("Stores Data")
        .master("local")
        .getOrCreate();
    // @formatter:on
  }

  private Dataset<Row> getPopulationDataframe() {
    String filename =
        "data/2010+Census+Population+By+Zipcode+(ZCTA).csv";
    Dataset<Row> df = spark.read().format("csv").option(
        "inferSchema", "true").option("header", "true")
        .load(filename);
    df = df.withColumnRenamed("Zip Code ZCTA", "zipcode");
    df = df.withColumnRenamed("2010 Census Population",
        "pop");
    df.show();
    return df;
  }

  private Dataset<Row> getHouseholdDataframe() {
    String filename = "data/14zpallagi.csv";
    Dataset<Row> df = spark.read().format("csv").option(
        "inferSchema", "true").option("header", "true")
        .load(filename);

    Dataset<Row> df2 = df.withColumn("cnt", df.col("N1")
        .multiply(df.col("agi_stub")));
    df2 = df2.drop("STATEFIPS");
    df2 = df2.drop("mars1");
    df2 = df2.drop("MARS2");
    df2 = df2.drop("MARS4");
    df2 = df2.drop("PREP");
    df2 = df2.drop("N2");
    df2 = df2.drop("NUMDEP");
    df2 = df2.drop("TOTAL_VITA");
    df2 = df2.drop("VITA");
    df2 = df2.drop("TCE");
    df2 = df2.drop("A00100");
    df2 = df2.drop("N02650");
    df2 = df2.drop("N00200");
    df2 = df2.drop("A00200");
    df2 = df2.drop("N00300");
    df2 = df2.drop("A00300");
    df2 = df2.drop("N00600");
    df2 = df2.drop("A00600");
    df2 = df2.drop("N00650");
    df2 = df2.drop("A00650");
    df2 = df2.drop("N00700");
    df2 = df2.drop("A00700");
    df2 = df2.drop("N00900");
    df2 = df2.drop("A00900");
    df2 = df2.drop("N01000");
    df2 = df2.drop("A01000");
    df2 = df2.drop("N01400");
    df2 = df2.drop("A01400");
    df2 = df2.drop("N01700");
    df2 = df2.drop("A01700");
    df2 = df2.drop("SCHF");
    df2 = df2.drop("N02300");
    df2 = df2.drop("A02300");
    df2 = df2.drop("N02500");
    df2 = df2.drop("A02500");
    df2 = df2.drop("N26270");
    df2 = df2.drop("A26270");
    df2 = df2.drop("N02900");
    df2 = df2.drop("A02900");
    df2 = df2.drop("N03220");
    df2 = df2.drop("A03220");
    df2 = df2.drop("N03300");
    df2 = df2.drop("A03300");
    df2 = df2.drop("N03270");
    df2 = df2.drop("A03270");
    df2 = df2.drop("N03150");
    df2 = df2.drop("A03150");
    df2 = df2.drop("N03210");
    df2 = df2.drop("A03210");
    df2 = df2.drop("N03230");
    df2 = df2.drop("A03230");
    df2 = df2.drop("N03240");
    df2 = df2.drop("A03240");
    df2 = df2.drop("N04470");
    df2 = df2.drop("A04470");
    df2 = df2.drop("A00101");
    df2 = df2.drop("N18425");
    df2 = df2.drop("A18425");
    df2 = df2.drop("N18450");
    df2 = df2.drop("A18450");
    df2 = df2.drop("N18500");
    df2 = df2.drop("A18500");
    df2 = df2.drop("N18300");
    df2 = df2.drop("A18300");
    df2 = df2.drop("N19300");
    df2 = df2.drop("A19300");
    df2 = df2.drop("N19700");
    df2 = df2.drop("A19700");
    df2 = df2.drop("N04800");
    df2 = df2.drop("A04800");
    df2 = df2.drop("N05800");
    df2 = df2.drop("A05800");
    df2 = df2.drop("N09600");
    df2 = df2.drop("A09600");
    df2 = df2.drop("N05780");
    df2 = df2.drop("A05780");
    df2 = df2.drop("N07100");
    df2 = df2.drop("A07100");
    df2 = df2.drop("N07300");
    df2 = df2.drop("A07300");
    df2 = df2.drop("N07180");
    df2 = df2.drop("A07180");
    df2 = df2.drop("N07230");
    df2 = df2.drop("A07230");
    df2 = df2.drop("N07240");
    df2 = df2.drop("A07240");
    df2 = df2.drop("N07220");
    df2 = df2.drop("A07220");
    df2 = df2.drop("N07260");
    df2 = df2.drop("A07260");
    df2 = df2.drop("N09400");
    df2 = df2.drop("A09400");
    df2 = df2.drop("N85770");
    df2 = df2.drop("A85770");
    df2 = df2.drop("N85775");
    df2 = df2.drop("A85775");
    df2 = df2.drop("N09750");
    df2 = df2.drop("A09750");
    df2 = df2.drop("N10600");
    df2 = df2.drop("A10600");
    df2 = df2.drop("N59660");
    df2 = df2.drop("A59660");
    df2 = df2.drop("N59720");
    df2 = df2.drop("A59720");
    df2 = df2.drop("N11070");
    df2 = df2.drop("A11070");
    df2 = df2.drop("N10960");
    df2 = df2.drop("A10960");
    df2 = df2.drop("N11560");
    df2 = df2.drop("A11560");
    df2 = df2.drop("N06500");
    df2 = df2.drop("A06500");
    df2 = df2.drop("N10300");
    df2 = df2.drop("A10300");
    df2 = df2.drop("N85530");
    df2 = df2.drop("A85530");
    df2 = df2.drop("N85300");
    df2 = df2.drop("A85300");
    df2 = df2.drop("N11901");
    df2 = df2.drop("A11901");
    df2 = df2.drop("N11902");
    df2 = df2.drop("A11902");
    df2 = df2.filter(df2.col("agi_stub").$greater(3));
    df2 = df2.groupBy("zipcode").sum("cnt");
    df2 = df2.withColumnRenamed("sum(cnt)", "household");
    df2.show();

    return df2;
  }

  private Dataset<Row> joinHouseholdPopulation(Dataset<
      Row> householdDf, Dataset<Row> populationDf) {
    Dataset<Row> df = householdDf.join(populationDf,
        householdDf.col("zipcode").equalTo(populationDf.col(
            "zipcode")), "outer").drop(populationDf.col(
                "zipcode"));
    df = df.withColumn("idx", df.col("household").divide(df
        .col("pop")));
    df.show();
    return df;
  }

  private Dataset<Row> getSalesData() {
    // Let's connect to the database
    Config config = ConfigManager.getConfig(K.INFORMIX);
    Connection connection = config.getConnection();
    if (connection == null) {
      return null;
    }

    // List of all tables we want to work with
    List<String> tables = new ArrayList();
    tables.add("customer");
    tables.add("orders");
    tables.add("items");
    tables.add("stock");

    // Specific Informix dialect
    JdbcDialect dialect = new InformixJdbcDialect();
    JdbcDialects.registerDialect(dialect);

    Map<String, Dataset<Row>> datalake = new HashMap<>();
    for (String table : tables) {
      System.out.print("Loading table [" + table
          + "] ... ");
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
        + " table in our data lake");

    // Let's look at the content
    for (String table : tables) {
      Dataset<Row> df = datalake.get(table);
      System.out.println("Number of rows in " + table + ": "
          + df.count());
      df.show(10);
      df.printSchema();
    }

    Dataset<Row> ordersDf = datalake.get("orders");
    // @formatter:off
    ordersDf = ordersDf.withColumn(
        "time_to_ship", 
        datediff(ordersDf.col("ship_date"), ordersDf.col("order_date")));
    // @formatter:on

    ordersDf.printSchema();
    ordersDf.show();
    System.out.println("We have " + ordersDf.count()
        + " orders");

    Dataset<Row> ordersDf2 = ordersDf.filter(
        "time_to_ship IS NOT NULL");
    ordersDf2.printSchema();
    ordersDf2.show();
    System.out.println("We have " + ordersDf2.count()
        + " delivered orders");

    Dataset<Row> customerDf = datalake.get("customer");
    Dataset<Row> itemsDf = datalake.get("items");
    Dataset<Row> stockDf = datalake.get("stock");

    Seq<String> stockColumns =
        new scala.collection.immutable.Set.Set2<>(
            "stock_num", "manu_code").toSeq();

    // @formatter:off
    Dataset<Row> allDf = customerDf
        .join(ordersDf, customerDf.col("customer_num").equalTo(ordersDf.col("customer_num")), "full_outer")
        .join(itemsDf, ordersDf.col("order_num").equalTo(itemsDf.col("order_num")), "full_outer")
        .join(stockDf, stockColumns, "full_outer")
        .drop(ordersDf.col("customer_num"))
        .drop(itemsDf.col("order_num"))
        .drop(stockDf.col("stock_num"))
        .drop(stockDf.col("manu_code"));
    // @formatter:on
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

    Dataset<Row> unsoldProductsDf = allDf.filter(
        "order_num IS NULL").filter(
            "description IS NOT NULL").drop(JavaConversions
                .asScalaBuffer(columnsToDrop));
    unsoldProductsDf.cache();
    System.out.println("We have " + unsoldProductsDf.count()
        + " unsold references in our warehouse, time to do something!");
    unsoldProductsDf.show();

    // Sales analysis
    Dataset<Row> salesDf = allDf.filter(
        "zipcode IS NOT NULL").groupBy("zipcode").sum(
            "total_price");
    salesDf = salesDf.withColumn("revenue", salesDf.col(
        "sum(total_price)")).drop("sum(total_price)")
        .filter("revenue IS NOT NULL");
    salesDf.show();
    return salesDf;
  }
}
