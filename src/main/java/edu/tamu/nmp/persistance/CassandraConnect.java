package edu.tamu.nmp.persistance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Class to connect to Cassandra DB
 *
 */
public class CassandraConnect {

	private Cluster cluster;

	private Session session;

	// Call this method to connect to cluster
	public void connect(final String node, final int port) {
		this.cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
		session = cluster.connect();
	}

	public Cluster getCluster() {
		return cluster;
	}

	public Session getSession() {
		return session;
	}

	public void close() {
		cluster.close();
		session.close();
	}
}
