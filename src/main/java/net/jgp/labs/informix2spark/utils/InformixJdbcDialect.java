/**
 * 
 */
package net.jgp.labs.informix2spark.utils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;

import com.informix.jdbc.IfxConnection;

import scala.Option;

/**
 * @author jgp
 *
 */
public class InformixJdbcDialect extends JdbcDialect {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6236667577063262901L;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.spark.sql.jdbc.JdbcDialect#canHandle(java.lang.String)
	 */
	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:informix-sqli");
	}

	@Override
	public Option<JdbcType> getJDBCType(DataType dt) {
		if (DataTypes.StringType.sameType(dt)) {
			return Option.apply(new JdbcType("SERIAL", java.sql.Types.INTEGER));
		}
		return Option.empty();
	}

	@Override
	public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
		if (typeName.toUpperCase().compareTo("SERIAL") == 0) {
			return Option.apply(DataTypes.StringType);
		}
		return Option.empty();
	}

	@Override
	public void beforeFetch(Connection connection, scala.collection.immutable.Map<String, String> properties) {
		super.beforeFetch(connection, properties);
	}
}
