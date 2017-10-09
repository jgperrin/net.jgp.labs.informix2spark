package net.jgp.labs.informix2spark.l400;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HouseholdsAboveMedianRevenuePerZipApp {

  public static void main(String[] args) {
    HouseholdsAboveMedianRevenuePerZipApp app =
        new HouseholdsAboveMedianRevenuePerZipApp();
    app.start(27514);
  }

  private void start(int zip) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Number of households in your ZIP Code™")
        .master("local")
        .getOrCreate();

    String filename = "data/14zpallagi-part*.csv";
    Dataset<Row> df = spark
        .read()
        .format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filename);
    df.printSchema();
    df.sample(true, 0.01, 4589).show(2);
    System.out.println("Dataframe has " + df.count() + " rows and " + df.columns().length
        + " columns.");

    Dataset<Row> df2 = df.filter(df.col("zipcode").equalTo(zip));
    String[] colsToDrop = { "STATEFIPS", "mars1", "MARS2", "MARS4", "PREP", "N2",
        "NUMDEP", "TOTAL_VITA", "VITA", "TCE", "A00100", "N02650", "N00200", "A00200",
        "N00300", "A00300", "N00600", "A00600", "N00650", "A00650", "N00700", "A00700",
        "N00900", "A00900", "N01000", "A01000", "N01400", "A01400", "N01700", "A01700",
        "SCHF", "N02300", "A02300", "N02500", "A02500", "N26270", "A26270", "N02900",
        "A02900", "N03220", "A03220", "N03300", "A03300", "N03270", "A03270", "N03150",
        "A03150", "N03210", "A03210", "N03230", "A03230", "N03240", "A03240", "N04470",
        "A04470", "A00101", "N18425", "A18425", "N18450", "A18450", "N18500", "A18500",
        "N18300", "A18300", "N19300", "A19300", "N19700", "A19700", "N04800", "A04800",
        "N05800", "A05800", "N09600", "A09600", "N05780", "A05780", "N07100", "A07100",
        "N07300", "A07300", "N07180", "A07180", "N07230", "A07230", "N07240", "A07240",
        "N07220", "A07220", "N07260", "A07260", "N09400", "A09400", "N85770", "A85770",
        "N85775", "A85775", "N09750", "A09750", "N10600", "A10600", "N59660", "A59660",
        "N59720", "A59720", "N11070", "A11070", "N10960", "A10960", "N11560", "A11560",
        "N06500", "A06500", "N10300", "A10300", "N85530", "A85530", "N85300", "A85300",
        "N11901", "A11901", "N11902", "A11902" };
    for (String colName : colsToDrop) {
      df2 = df2.drop(colName);
    }
    df2.printSchema();
    df2.show();
    System.out.println("Dataframe has " + df2.count() + " rows and " + df2
        .columns().length + " columns.");

    Dataset<Row> df3 = df2.filter(df2.col("agi_stub").$greater(3));
    df3 = df3.groupBy("zipcode").sum("N1").withColumnRenamed("sum(N1)", "households");
    df3.show();
  }
}
