package net.jgp.labs.informix2spark.utils;

public abstract class Config {

	private String database;
	private String hostname;
	private String password;
	private int port;
	private String table;
	private String user;
	private String databaseServer;

	public String getDatabase() {
		return database;
	}

	public abstract String getDriver();

	public String getHostname() {
		return hostname;
	}

	public String getPassword() {
		return password;
	}

	public int getPort() {
		return port;
	}

	public String getTable() {
		return table;
	}

	public String getUser() {
		return user;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setDatabaseServer(String databaseServer) {
		this.databaseServer = databaseServer;
	}

	public abstract String getJdbcUrl();

	public String getDatabaseServer() {
		return databaseServer;
	}

}
