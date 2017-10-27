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
		PreparedStatement ps = session.prepare("Insert into nflix.movies(id, movie_id, year, movie_name, genre1, genre2, genre3) values (now(),?,?,?,?,?,?)");
		BoundStatement bs;
		try {
			
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				String val = "";
				String[] values = line.split(cvsSplit);
				for(int i = 0; i<values.length; i++) {
					values[i] = values[i].replaceAll("^\"|\"$", "");
				}
				
				if(values.length > 5) {
					val = values[5];
				}
				bs = ps.bind(new Integer(values[0]),new Integer(values[1]),values[2],values[3],values[4],val);
				session.execute(bs);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("Movies Data Inserted");
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
		final String csvFile = "src/data/rating_final.csv";
		BufferedReader br = null;
		String line = "";
		final String cvsSplit = ",";
		PreparedStatement ps = session.prepare("Insert into nflix.ratings(id, movie_id, user_id, rating, date) values (now(),?,?,?,?)");
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
			System.out.println("Ratings Data Inserted");
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}   
	}

	private static void deleteKeySpace(Session session) {
	String cql = "DROP KEYSPACE IF EXISTS NFLIX";
	session.execute(cql);
	}
	// Create a keyspace
	private static void createKeySpace(Session session) {
		String cql = "CREATE KEYSPACE IF NOT EXISTS NFLIX "+
	                  "WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3}";
		session.execute(cql);
	}

	// Create Movie Table
	private static void createMovieTable(Session session) {
		String cql = "CREATE TABLE IF NOT EXISTS NFLIX.MOVIES(ID timeuuid, MOVIE_ID INT, YEAR INT, MOVIE_NAME VARCHAR, GENRE1 VARCHAR, GENRE2 VARCHAR, GENRE3 VARCHAR, PRIMARY KEY(ID, MOVIE_ID))";
		session.execute(cql);
	}

	// Create Rating Table
	private static void createRatingTable(Session session) {
		String cql = "CREATE TABLE IF NOT EXISTS NFLIX.RATINGS(ID timeuuid, MOVIE_ID INT, USER_ID INT, RATING VARCHAR, DATE VARCHAR, PRIMARY KEY(ID, MOVIE_ID))";
		session.execute(cql);
	}

	public static void main(String args[]) {
		CassandraConnect cc = new CassandraConnect();
		cc.connect("localhost", 9042);
		Session session = cc.getSession();
		deleteKeySpace(session);
		createKeySpace(session);
		createMovieTable(session);
		insertMovieRecords(session);
		createRatingTable(session);
	    insertRatingRecords(session);
		session.close();
	}
}
