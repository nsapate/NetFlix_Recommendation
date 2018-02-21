package edu.tamu.nmp.persistance;

import com.datastax.driver.core.Session;

/**
 * This class returns a Cassandra database session
 * 
 * @author team-7
 */
public class SessionConnector {
	Session session;

	// Return a new session
	public Session getSession() {
		CassandraConnect cc = new CassandraConnect();
		cc.connect("localhost", 9042);
		session = cc.getSession();
		return session;
	}

}
