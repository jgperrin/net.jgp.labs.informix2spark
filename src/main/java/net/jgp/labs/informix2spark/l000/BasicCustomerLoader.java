package net.jgp.labs.informix2spark.l000;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BasicCustomerLoader {

  public static void main(String[] args) {
    BasicCustomerLoader app = new BasicCustomerLoader();
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

    // @formatter:off
    Dataset<Row> df = spark
      .read()
      .format("jdbc")
      .option(
          "url", 
          "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y")
      .option("dbtable", "customer")
      .option("user", "informix")
      .option("password", "in4mix")
      .load();
    // @formatter:on

    df.printSchema();
    System.out.println("Number of rows in customer: " + df
        .count());
    df.show(5);
  }
}
