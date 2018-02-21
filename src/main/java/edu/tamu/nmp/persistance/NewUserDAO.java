package edu.tamu.nmp.persistance;

import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import edu.tamu.nmp.domain.Movie;
import edu.tamu.nmp.domain.User;

/**
 * DAO class for inserting new user details
 * 
 * @author team7
 */
public class NewUserDAO {

	private String cql_user = "INSERT INTO NFLIX.NEWUSER(USER_ID, NAME, GENRE1, GENRE2, RATING) VALUES (?,?,?,?,?)";
	private Session session;

	public void init() {
		SessionConnector sc = new SessionConnector();
		session = sc.getSession();
	}

	public void insertNewUser(User user) {
		init();
		PreparedStatement ps = session.prepare(cql_user);
		BoundStatement bs = ps.bind(user.getUser_id(), user.getName(), user.getGenre1(), user.getGenre2(), user.getRating());
		session.execute(bs);
	}

	public void recommendNewUserMovies(String genre1, String genre2) {
		MovieRecommendationDAO movieDAO = new MovieRecommendationDAO();
		List<Movie> movieList = movieDAO.getSpecificGenreMovies(genre1, genre2);
		if (movieList.isEmpty()) {
			movieList = movieDAO.getMoviesFromGenre(genre1);
		}
		int i = 0;
		for (Movie movie : movieList) {
			System.out.println("----------------------------");
			System.out.println("Movie Name:" + movie.getName());
			System.out.println("Year:" + movie.getYear());
			if (++i > 10)
				break;
		}
	}
}
