package net.jgp.labs.informix2spark.utils;

public class ConfigManager {

	public static Config getConfig(String db) {
		Config c = null;
		
		switch (db) {
		case K.INFORMIX:
			c = new InformixConfig();
			c.setHostname("[::1]");
			c.setPort(33378);
			c.setUser("informix");
			c.setPassword("in4mix");
			c.setDatabase("stores_demo");
			c.setDatabaseServer("lo_informix1210");
			c.setTable("customer");
			break;

		case K.MYSQL:
			c = new MySqlConfig();
			c.setHostname("localhost");
			c.setPort(3306);
			c.setUser("root");
			c.setPassword("password");
			c.setDatabase("sakila");
			c.setTable("actor");
			break;

		default:
			break;
		}
		return c;
	}

}
