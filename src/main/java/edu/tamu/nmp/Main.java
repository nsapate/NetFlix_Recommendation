package edu.tamu.nmp;

import java.util.Scanner;

import com.datastax.driver.core.Session;

import edu.tamu.nmp.persistance.CassandraConnect;

/**
 * Main Class for netflix recommendation. 
 * @author team 7
 *
 */
public class Main {
	public static void main(String[] args) {
	/*	System.out.println("Starting Netflix recommendation system");
		CassandraConnect cc = new CassandraConnect();
		cc.connect("localhost", 9042);
		Session session = cc.getSession();
//		runQuery(session); // run all the queries
		session.close();
	*/
		 Scanner scanner = new Scanner(System.in);

	        while (true) {

	            System.out.print("Enter user ID : ");
	            String input = scanner.nextLine();

	            if ("q".equals(input) ) {
	                System.out.println("Exit!");
	                break;
	            }
	            else if("".equals(input)) {
	            	System.out.println("Please enter User ID\n");
	            	continue;
	            }

	            System.out.println("input : " + input);
	            System.out.println("-----------\n");
	        }

	        scanner.close();

	    }
	}
/*
	// This method runs all queries
	private static void runQuery(Session session) {
		
		// local variables
		String cql= null;
		ResultSet results = null;
		String movie_id = null;
		String movie_name = null;
		String year = null;
		String rating= null;
		String user_id=null;
			
		System.out.println("Movie data \n"); // query to fetch all movie details
		 cql = "select * from NFLIX.MOVIES";
		session.execute(cql);
		results = session.execute(cql);
		
		for (final Row row : results) {
		movie_id = Integer.toString(row.getInt("movie_id"));
		movie_name = row.getString("movie_name");
		year = Integer.toString(row.getInt("year"));	
		System.out.println("Movie ID: " + movie_id + " Name: " + movie_name + " Year: " + year);	
		}
		
		System.out.println("\n User Ratings \n"); // query to fetch all user details
		cql = "select rating,user_id from NFLIX.RATINGS";
		session.execute(cql);
		results = session.execute(cql);
		
		for (final Row row : results) {
		user_id = Integer.toString(row.getInt("user_id"));
		rating = row.getString("rating");
		System.out.println("User ID: " + user_id + " Rating: " + rating);
		}
	
		// Clean up the connection by closing it
		session.close();
		
	}*/

