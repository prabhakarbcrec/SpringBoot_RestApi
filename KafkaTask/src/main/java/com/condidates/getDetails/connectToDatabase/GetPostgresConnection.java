package com.condidates.getDetails.connectToDatabase;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.condidates.getDetails.cost.AppConstants;

public class GetPostgresConnection {
	private static Connection con = null;

	public GetPostgresConnection()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName(AppConstants.DRIVER);

		con = DriverManager.getConnection(AppConstants.POSTGRES_PATH_TO_CONNECT_LOCAL_DB,
				AppConstants.POSTGRES_USER_NAME, AppConstants.POSTGRES_PASSWORD);

		System.out.println("New Connection created with Postgres:");
	}

	public static Connection getConnectionInstance()
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		if (con == null) {
			synchronized (GetPostgresConnection.class) {
				new GetPostgresConnection();

			}
		}

		return con;

	}

}
