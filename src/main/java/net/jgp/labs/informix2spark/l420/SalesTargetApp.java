package net.jgp.labs.informix2spark.l420;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.datediff;
import static org.apache.spark.sql.functions.lit;

import java.math.BigDecimal;
import java.sql.Connection;
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

public class SalesTargetApp {

  SparkSession spark;

  public static void main(String[] args) {
    SalesTargetApp app = new SalesTargetApp();
    app.start();
  }

  public SalesTargetApp() {
    init();
  }

  private void init() {
    this.spark = SparkSession
        .builder()
        .appName("Sales Target")
        .master("local")
        .getOrCreate();
  }

  private void start() {
    Dataset<Row> householdDf = getHouseholdDataframe();
    Dataset<Row> populationDf = getPopulationDataframe();
    Dataset<Row> indexDf = joinHouseholdPopulation(householdDf, populationDf);
    Dataset<Row> salesDf = getSalesData();

    Dataset<Row> salesIndexDf = salesDf
        .join(indexDf, salesDf.col("zipcode").equalTo(indexDf.col("zipcode")), "left")
        .drop(indexDf.col("zipcode"));
    salesIndexDf = salesIndexDf.withColumn("revenue_by_inh", salesIndexDf.col("revenue")
        .divide(salesIndexDf.col("pop")));
    salesIndexDf = salesIndexDf.orderBy(col("revenue_by_inh").desc());
    Row bestRow = salesIndexDf.first();
    double bestRevenuePerInhabitant = ((BigDecimal) bestRow.getAs("revenue_by_inh"))
        .doubleValue();
    int populationOfBestRevenuePerInhabitant = bestRow.getAs("pop");
    double incomeOfBestRevenuePerInhabitant = bestRow.getAs("income_per_inh");
    salesIndexDf = salesIndexDf.withColumn(
        "best_revenue_per_inh",
        salesIndexDf.col("pop").divide(salesIndexDf.col("pop"))
            .multiply(bestRevenuePerInhabitant));
    salesIndexDf = salesIndexDf.withColumn(
        "pop_of_best",
        lit(populationOfBestRevenuePerInhabitant));
    salesIndexDf = salesIndexDf.withColumn(
        "income_of_best",
        lit(incomeOfBestRevenuePerInhabitant));
    salesIndexDf = salesIndexDf.withColumn(
        "idx_revenue",
        salesIndexDf.col("best_revenue_per_inh")
            .divide(salesIndexDf.col("revenue_by_inh")));
    salesIndexDf = salesIndexDf.withColumn(
        "idx_pop",
        salesIndexDf.col("pop").divide(salesIndexDf.col("pop_of_best")));
    salesIndexDf = salesIndexDf.withColumn(
        "idx_income",
        salesIndexDf.col("income_per_inh").divide(salesIndexDf.col(
            "income_of_best")));
    salesIndexDf = salesIndexDf.withColumn(
        "index",
        salesIndexDf.col("idx_revenue").multiply(salesIndexDf.col("idx_pop")
            .multiply(salesIndexDf.col("idx_income"))));
    salesIndexDf = salesIndexDf.withColumn(
        "potential_revenue",
        salesIndexDf.col("revenue").multiply(salesIndexDf.col("index")));
    salesIndexDf = salesIndexDf
        .drop("idx_income")
        .drop("idx_pop")
        .drop("idx_revenue")
        .drop("income_of_best")
        .drop("pop_of_best")
        .drop("best_revenue_per_inh")
        .orderBy(salesIndexDf.col("potential_revenue").desc());
    salesIndexDf.show();
    System.out.println("Max revenue per inhabitant: $" + bestRevenuePerInhabitant
        + "/inh");
  }

  private Dataset<Row> getPopulationDataframe() {
    String filename = "data/2010+Census+Population+By+Zipcode+(ZCTA).csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    df = df.withColumnRenamed("Zip Code ZCTA", "zipcode");
    df = df.withColumnRenamed("2010 Census Population", "pop");
    df.show();
    return df;
  }

  private Dataset<Row> getHouseholdDataframe() {
    String filename = "data/14zpallagi*.csv";
    Dataset<Row> df = spark.read().format("csv")
        .option("inferSchema", "true")
        .option("header", "true").load(filename);

    df = df.select(
        df.col("zipcode"),
        df.col("agi_stub"),
        df.col("N1"),
        df.col("A02650"),
        df.col("N1").multiply(df.col("A02650")));
    df = df.withColumnRenamed(df.columns()[df.columns().length - 1], "r");
    df = df.groupBy("zipcode").sum("r");
    df = df.withColumnRenamed(df.columns()[df.columns().length - 1], "total_income");

    return df;
  }

  private Dataset<Row> joinHouseholdPopulation(
      Dataset<Row> householdDf,
      Dataset<Row> populationDf) {
    Dataset<Row> df = householdDf
        .join(
            populationDf,
            householdDf.col("zipcode").equalTo(populationDf.col("zipcode")),
            "outer")
        .drop(populationDf.col("zipcode"))
        .withColumn(
            "income_per_inh",
            householdDf.col("total_income").divide(populationDf.col("pop")));
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
    List<String> tables = new ArrayList<>();
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
