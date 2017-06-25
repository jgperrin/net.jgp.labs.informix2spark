package net.jgp.labs.informix2spark.l010;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

import net.jgp.labs.informix2spark.utils.InformixJdbcDialect;

public class CustomerLoader {

	public static void main(String[] args) {
		CustomerLoader app = new CustomerLoader();
		app.start();
	}

	private void start() {
		SparkSession spark = SparkSession.builder().appName("Stores Customer").master("local").getOrCreate();

		JdbcDialect dialect = new InformixJdbcDialect();
		JdbcDialects.registerDialect(dialect);

		String hostname = "[::1]";
		int port = 33378;
		String user = "informix";
		String password = "in4mix";
		String database = "stores_demo";
		String informixServer = "lo_informix1210";
		String jdbcUrl = "jdbc:informix-sqli://" + hostname + ":" + port + "/" + database + ":INFORMIXSERVER="
				+ informixServer;
		String table = "informix.customer";
		String driver = "com.informix.jdbc.IfxDriver";

		// @formatter:off
		Dataset<Row> df = spark
				.read()
				.format("jdbc")
				.option("url", jdbcUrl)
				.option("dbtable", table)
				.option("user", user)
				.option("password", password)
				.option("driver", driver)
				.load();
		// @formatter:on

		df.cache();
		df.printSchema();
		System.out.println("Number of rows in " + table + ": " + df.count());
		df.show();
	}
}
