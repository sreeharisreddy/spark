package com.sree.hive;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HiveServiceImpl {

	@Autowired HiveConnectionFactory hiveConnectionFactory;
	Logger logger = LoggerFactory.getLogger(HiveServiceImpl.class);
	
	public void execute(String query) throws SQLException {
		
		Connection connection = hiveConnectionFactory.getNewConnection();
		Statement statement = connection.createStatement();
		statement.executeQuery(query);
		connection.close();
	}
	
	public int executeUpdate(String query) throws SQLException {
		logger.debug("executoing the query for {} ",query);
		Connection connection = hiveConnectionFactory.getNewConnection();
		Statement statement = connection.createStatement();
		int executeUpdate = statement.executeUpdate(query);
		connection.close();
		logger.info("Executed Successfully ");
		return executeUpdate;
	}
	
	
}
