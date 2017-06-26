package net.jgp.labs.informix2spark.utils;

public class InformixConfig extends Config {

	@Override
	public String getDriver() {
		return "com.informix.jdbc.IfxDriver";
	}

	@Override
	public String getJdbcUrl() {
		String jdbcUrl = "jdbc:informix-sqli://" + getHostname() + ":" + getPort() + "/" + getDatabase() + ":IFXHOST="
				+ super.getDatabaseServer() + ";DELIMIDENT=Y";

		return jdbcUrl;
	}
}
