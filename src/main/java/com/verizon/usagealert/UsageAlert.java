package com.verizon.usagealert;

import java.util.HashMap;
import java.util.Set;

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
			HashMap alertmap = new HashMap();
			alertmap = doAlert(session);
			Set keys = alertmap.keySet();
			for (Object key : keys) {
				System.out.println(key);
				alertCumption((int) key,(int)alertmap.get(key),session);	
			}
		} finally {
			session.close();
			cluster.close();
		}
	}
	
	private static HashMap doAlert(Session session) {
		HashMap allalert = new HashMap();
		String alertQuery ="select usage, no_of_Notification from DATA_USAGE.CONSUMPTION_ALERT";
		ResultSet rs = session.execute(alertQuery);
		for (Row row:rs){
			int usage = row.getInt("usage");
			int noNotification = row.getInt("no_of_Notification");
			allalert.put(usage, noNotification);
		}
		return allalert;
	}

	private static void alertCumption(int range, int noofAlert, Session sess) {
		String custIdcump = null;
		String query = "select customerId, no_of_notification from DATA_USAGE.USER_PROFILE where perconsup = " + range
				+ " and no_of_notification < " + noofAlert + " ALLOW FILTERING";
		ResultSet rs1 = sess.execute(query);
		for (Row row : rs1) {
			custIdcump = row.getString("customerId");
			System.out.println("Alert this customer:::" + custIdcump + "::;");
			updateCumption(sess,custIdcump,noofAlert);
		}
	}
	
	private static void updateCumption(Session sess, String custIdCump, int i) {
		int newal = i + 1;
		sess.execute("update user_profile set no_of_Notification=" + newal + " where customerId='" + custIdCump + "'");
	}
}
