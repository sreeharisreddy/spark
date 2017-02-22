package com.sree.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.sree.util.SreePropertyUtil;

@Component
public class HiveConnectionFactory {

	Logger logger = LoggerFactory.getLogger(HiveConnectionFactory.class);
	@Autowired ApplicationContext context;
	
	private Connection connection;
	
	public synchronized Connection getSharedConnection() throws SQLException{
		if(connection == null || connection.isClosed()){
			connection = openConnection();
		}
		return connection;
	}
	
	public Connection getNewConnection() throws SQLException{
		return this.openConnection();
	}

	private Connection openConnection() throws SQLException {
		
		String driverClassName = SreePropertyUtil.getProperty("org.hive.jdbc.driver");
		String connectionUrl =  SreePropertyUtil.getProperty("org.hive.connection.url");
		logger.trace("getting the connection using driver {} and connectionUrl {} ",driverClassName,connectionUrl);
		
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.getMessage(),e);
			throw new RuntimeException();
		}
		Connection connection = DriverManager.getConnection(connectionUrl);
		return connection;
	}
	
}
