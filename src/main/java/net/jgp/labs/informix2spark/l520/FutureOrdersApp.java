package net.jgp.labs.informix2spark.l520;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.weekofyear;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

import net.jgp.labs.informix2spark.utils.Config;
import net.jgp.labs.informix2spark.utils.ConfigManager;
import net.jgp.labs.informix2spark.utils.InformixJdbcDialect;
import net.jgp.labs.informix2spark.utils.K;

public class FutureOrdersApp {

  public static void main(String[] args) {
    FutureOrdersApp app = new FutureOrdersApp();
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
    spark.udf().register("vectorBuilder", new VectorBuilderInteger(), new VectorUDT());

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
    allDf.show(10);

    Dataset<Row> df = allDf
        .withColumn("values_for_features", allDf.col("order_week"))
        .withColumn("label", allDf.col("sum(total_price)"))
        .withColumn("features", callUDF("vectorBuilder", col("values_for_features")))
        .drop(col("values_for_features"));
    df.printSchema();
    df.show();

    LinearRegression lr = new LinearRegression().setMaxIter(20);

    // Fit the model to the data.
    LinearRegressionModel model = lr.fit(df);

    // Given a dataset, predict each point's label, and show the results.
    model.transform(df).show();

    LinearRegressionTrainingSummary trainingSummary = model.summary();
    System.out.println("numIterations: " + trainingSummary.totalIterations());
    System.out.println("objectiveHistory: " +
        Vectors.dense(trainingSummary.objectiveHistory()));
    trainingSummary.residuals().show();
    System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
    System.out.println("r2: " + trainingSummary.r2());

    double intercept = model.intercept();
    System.out.println("Intersection: " + intercept);
    double regParam = model.getRegParam();
    System.out.println("Regression parameter: " + regParam);
    double tol = model.getTol();
    System.out.println("Tol: " + tol);

    for (double feature = 31.0; feature < 34; feature++) {
      Vector features = Vectors.dense(feature);
      double p = model.predict(features);

      System.out.printf("Total orders prediction for week #%d is $%4.2f.\n",
          Double.valueOf(feature).intValue(), 
          p);
    }
  }
}
