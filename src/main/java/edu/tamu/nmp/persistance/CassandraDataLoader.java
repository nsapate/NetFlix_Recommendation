package edu.tamu.nmp.persistance;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * This class creates the keyspace, tables and populates data
 * @author team7
 *
 */
public class CassandraDataLoader {

	// Insert data into Movie Table
	private static void insertMovieRecords(Session session) {
		System.out.println("Inserting Data into Cassandra DB for Movies");
		final String csvFile = "src/data/movie_titles.csv";
		BufferedReader br = null;
		String line = "";
		final String cvsSplit = ",";
		PreparedStatement ps = session.prepare("Insert into nflix.movies(movie_id, year, movie_name) values (?,?,?)");
		BoundStatement bs;
		try {
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				String[] values = line.split(cvsSplit);
				bs = ps.bind(new Integer(values[0]),new Integer(values[1]),values[2]);
				session.execute(bs);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}   
	}
	
	// Insert Data in Records Table
	private static void insertRatingRecords(Session session) {
		System.out.println("Inserting Data into Cassandra DB for Ratings");
		final String csvFile = "src/data/ratings_dataset.csv";
		BufferedReader br = null;
		String line = "";
		final String cvsSplit = ",";
		PreparedStatement ps = session.prepare("Insert into nflix.ratings(movie_id, user_id, rating, date) values (?,?,?,?)");
		BoundStatement bs;
		try {
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				String[] values = line.split(cvsSplit);
				bs = ps.bind(new Integer(values[0]),new Integer(values[1]),values[2].toString(),values[3].toString());
				session.execute(bs);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}   
	}

	// Create a keyspace
	private static void createKeySpace(Session session) {
		String cql = "CREATE KEYSPACE IF NOT EXISTS NFLIX "+
	                  "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3}";
		session.execute(cql);
	}

	// Create Movie Table
	private static void createMovieTable(Session session) {
		String cql = "CREATE TABLE IF NOT EXISTS NFLIX.MOVIES(MOVIE_ID INT PRIMARY KEY, YEAR INT, MOVIE_NAME VARCHAR)";
		session.execute(cql);
	}

	// Create Rating Table
	private static void createRatingTable(Session session) {
		String cql = "CREATE TABLE IF NOT EXISTS NFLIX.RATINGS(MOVIE_ID INT, USER_ID INT PRIMARY KEY, RATING VARCHAR, DATE VARCHAR)";
		session.execute(cql);
	}

	public static void main(String args[]) {
		CassandraConnect cc = new CassandraConnect();
		cc.connect("localhost", 9042);
		Session session = cc.getSession();
		createKeySpace(session);
		createMovieTable(session);
		insertMovieRecords(session);
		createRatingTable(session);
	    insertRatingRecords(session);
		session.close();
	}
}
