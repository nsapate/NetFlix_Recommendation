package edu.tamu.nmp.persistance;

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * This class fetches data for user preferences
 * 
 * @author Team 7
 */
public class UserPreferenceDAO {

	private String cql_ratings = "SELECT MOVIE_ID, RATING FROM NFLIX.RATINGS WHERE USER_ID=? ALLOW FILTERING";
	private String cql_movies = "SELECT MOVIE_NAME,GENRE1 FROM NFLIX.MOVIES WHERE MOVIE_ID= ? ALLOW FILTERING";
	private String cql_userTable = "SELECT GENRE1,GENRE2 FROM NFLIX.NEWUSER WHERE USER_ID= ? ALLOW FILTERING";
	private Session session;

	public void init() {
		SessionConnector sc = new SessionConnector();
		session = sc.getSession();
	}

	// Display the Recommended movies history on basis of if user is new or old
	public int getMovieRecommendationHstry(String id) {
		init();
		PreparedStatement ps = session.prepare(cql_ratings);
		try{
			BoundStatement bs = ps.bind(new Integer(id));
			ResultSet rs = session.execute(bs);
		
		if (rs.isExhausted()) {
			ps = session.prepare(cql_userTable);
			bs = ps.bind(new Long(id));
			rs = session.execute(bs);
			if (rs.isExhausted()) {
				System.out.println("User ID does not exist, Enter existing user ID [ 123456 or 56565 ] or create a new user - Good Bye");
				return -1;
			}
			System.out.println("Hello, Welcome user " + id);
			Row row = rs.one();
			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Hey User, Looks like you haven't watched any movies yet. Let's recommend you some..");
			System.out.println("Loading..........");
			printMovieForNewUser(row.getString("genre1"), row.getString("genre2"));
		}
		while (!rs.isExhausted()) {
			Row row = rs.one();
			this.printMovieDetails(new Integer(row.getInt("movie_id")));
			System.out.println("  Rating         |      " + row.getInt("rating"));
			System.out.println("--------------------------------------------------");
			System.out.println();
			System.out.println();
		}
		return 1;
			}catch(NumberFormatException e) {
			System.out.println("Incorrect User ID. Please re enter");
			return -1;
		}
	}

	private void printMovieForNewUser(String genre1, String genre2) {
		NewUserDAO recommendedMovies = new NewUserDAO();
		recommendedMovies.recommendNewUserMovies(genre1, genre2);
	}

	private void printMovieDetails(Integer id) {
		PreparedStatement ps = session.prepare(cql_movies);
		BoundStatement bs = ps.bind(id);
		ResultSet rs = session.execute(bs);
		Row row = rs.one();
		System.out.println("--------------------------------------------------");
		System.out.println("  Movie Name     |     " + row.getString(0));
		System.out.println("  Genre          |     " + row.getString("Genre1"));
	}

}
