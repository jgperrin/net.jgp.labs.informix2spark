package net.jgp.labs.informix2spark.l400;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MedianRevenueInYourZipCode {

  public static void main(String[] args) {
    System.out.println("Working directory = " + System
        .getProperty("user.dir"));
    MedianRevenueInYourZipCode app =
        new MedianRevenueInYourZipCode();
    app.start(90011);
  }

  private void start(int zip) {
    SparkSession spark = SparkSession.builder().appName(
        "Median Revenue in your ZIP Code™").master("local")
        .getOrCreate();

    String filename = "data/14zpallagi.csv";
    Dataset<Row> df = spark.read().format("csv").option(
        "inferSchema", "true").option("header", "true")
        .load(filename);
    df.show();

    Dataset<Row> df2 = df.filter(df.col("zipcode").equalTo(
        zip));
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
    df2 = df2.withColumn("cnt", df2.col("N1").multiply(df2.col("agi_stub")));
    df2 = df2.filter(df2.col("agi_stub").$greater(3));
    df2 = df2.groupBy("zipcode").sum("cnt").as("households");
    df2.show();
  }
}
