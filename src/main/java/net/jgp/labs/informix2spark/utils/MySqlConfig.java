package net.jgp.labs.informix2spark.utils;

public class MySqlConfig extends Config {

	public MySqlConfig() {
		setPort(3306);
	}

	@Override
	public String getDriver() {
		return "com.mysql.cj.jdbc.Driver";
	}

	@Override
	public String getJdbcUrl() {
		// @formatter:off
		String jdbcUrl = "jdbc:mysql://" + getHostname() + ":" + getPort() + "/" + getDatabase() 
		        + "?user=" + getUser() 
				+ "&password=" + getPassword() 
				+ "&useUnicode=true"
				+ "&useJDBCCompliantTimezoneShift=true"
				+ "&useLegacyDatetimeCode=false"
				+ "&serverTimezone=UTC";
		// @formatter:on
		return jdbcUrl;
	}

}
