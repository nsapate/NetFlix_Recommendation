package edu.tamu.nmp.persistance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.tamu.nmp.domain.Movie;

/**
 * This DAO class return a lost of movie based on users' choice and preferences.
 * 
 * @author team7
 */
public class MovieRecommendationDAO {
	private String cql_FetchMovies = "SELECT * FROM NFLIX.MOVIES";
	private String cql_getRating = "Select AVG(RATING) FROM NFLIX.RATINGS WHERE USER_ID =? ALLOW FILTERING";
	private String cql_newRating = "Select RATING FROM NFLIX.NEWUSER WHERE USER_ID =?";

	private List<Movie> movies;
	private static Session session;

	public void init() {
		SessionConnector sc = new SessionConnector();
		session = sc.getSession();
	}

	// Return a list of Movies with user preference
	public List<Movie> getMoviesFromGenre(String genre) {
		init();
		ResultSet rs = session.execute(cql_FetchMovies);
		movies = new ArrayList<Movie>();
		Movie mov;
		while (!rs.isExhausted()) {
			Row row = rs.one();
			if (row.getString("GENRE1").toLowerCase().contains(genre)
					|| row.getString("GENRE2").toLowerCase().contains(genre)
					|| row.getString("GENRE3").toLowerCase().contains(genre)) {
				mov = new Movie();
				mov.setId(row.getInt("MOVIE_ID"));
				mov.setYear(row.getInt("YEAR"));
				mov.setName(row.getString("MOVIE_NAME"));
				mov.setGenre1(row.getString("GENRE1"));
				mov.setGenre2(row.getString("GENRE2"));
				mov.setGenre3(row.getString("GENRE3"));
				System.out.println();
				movies.add(mov);
			}
		}
		Collections.shuffle(movies);
		return movies;
	}

	//Get movies based on genre given
	public List<Movie> getSpecificGenreMovies(String genre1, String genre2) {
		init();
		ResultSet rs = session.execute(cql_FetchMovies);
		movies = new ArrayList<Movie>();
		Movie mov;
		HashSet<String> genres;
		while (!rs.isExhausted()) {
			Row row = rs.one();
			genres = new HashSet<>();
			genres.add(row.getString("GENRE1").toLowerCase());
			genres.add(row.getString("GENRE2").toLowerCase());
			genres.add(row.getString("GENRE3").toLowerCase());
			if (genres.contains(genre1) && genres.contains(genre2)) {
				mov = new Movie();
				mov.setId(row.getInt("MOVIE_ID"));
				mov.setYear(row.getInt("YEAR"));
				mov.setName(row.getString("MOVIE_NAME"));
				mov.setGenre1(row.getString("GENRE1"));
				mov.setGenre2(row.getString("GENRE2"));
				mov.setGenre3(row.getString("GENRE3"));
				System.out.println();
				movies.add(mov);
			}

		}
		Collections.shuffle(movies);
		return movies;
	}

	public int getPreferredRatingForUser(Integer id) {
		init();
		int val = 0;
		PreparedStatement ps = session.prepare(cql_getRating);
		BoundStatement bs = ps.bind(id);
		ResultSet rs = session.execute(bs);
		while (!rs.isExhausted()) {
			Row row = rs.one();
			val = row.getInt(0);
		}
		if(val == 0) {
			ps = session.prepare(cql_newRating);
			bs = ps.bind(id.longValue());
			rs = session.execute(bs);
			while (!rs.isExhausted()) {
				Row row = rs.one();
				val = row.getInt(0);
			}
		}
		return val;
	}
}
