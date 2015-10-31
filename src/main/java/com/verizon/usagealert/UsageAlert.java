package com.verizon.usagealert;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class UsageAlert {

	public static void main(String[] args) {
		Cluster cluster = null;
		Session session = null;
		try {
			cluster = Cluster.builder().addContactPoint("113.128.163.213").build();
			session = cluster.connect("DATA_USAGE");
			alertCumption(22, 0, session);
		} finally {
			session.close();
			cluster.close();

		}
	}
	
	private static void alertCumption(int range, int noofAlert, Session sess) {
		String custIdcump = null;
		String query = "select customerId, no_of_notification from DATA_USAGE.USER_PROFILE where perconsup = " + range
				+ " and no_of_notification = " + noofAlert + " ALLOW FILTERING";
		System.out.println("final " + query);
		ResultSet rs1 = sess.execute(query);
		for (Row row : rs1) {
			custIdcump = row.getString("customerId");
			System.out.println("Alert this customer:::" + custIdcump + "::;");
		}

	}
}
