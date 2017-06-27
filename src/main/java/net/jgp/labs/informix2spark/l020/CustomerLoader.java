package net.jgp.labs.informix2spark.l020;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import net.jgp.labs.informix2spark.utils.Config;
import net.jgp.labs.informix2spark.utils.ConfigManager;
import net.jgp.labs.informix2spark.utils.K;

public class CustomerLoader {

  public static void main(String[] args) {
    CustomerLoader app = new CustomerLoader();
    app.start();
  }

  private void start() {
    // @formatter:off
    SparkSession spark = SparkSession
        .builder()
        .appName("Stores Customer")
        .master("local")
        .getOrCreate();
    // @formatter:on

    Config config = ConfigManager.getConfig(K.INFORMIX);

    // @formatter:off
    Dataset<Row> df = spark
	.read()
	.format("jdbc")
	.option("url", config.getJdbcUrl())
	.option("dbtable", config.getTable())
	.option("user", config.getUser())
	.option("password", config.getPassword())
	.option("driver", config.getDriver())
	.load();
    // @formatter:on

    df.cache();
    df.printSchema();
    System.out.println("Number of rows in " + config
        .getTable() + ": " + df.count());
    df.show();
  }
}
