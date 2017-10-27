package edu.tamu.nmp.persistance;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import domain.Movie;

public class MovieRecommendationDAO {
	public static int counter = 0;
	private String cql = "SELECT * FROM NFLIX.MOVIES";
	
	private List<Movie> movies;
	private static Session session;
	
	public void init() {
		CassandraConnect cc = new CassandraConnect();
		cc.connect("localhost", 9042);
		session = cc.getSession();
	}
	 
	
	public List<Movie> getMoviesFromGenre(String genre){
		ResultSet rs = session.execute(cql);
		movies = new ArrayList<Movie>();
		Movie mov;
		while(!rs.isExhausted()) {
			Row row = rs.one();
			if(row.getString("GENRE1").contains(genre) || row.getString("GENRE2").contains(genre) || row.getString("GENRE3").contains(genre)){
				mov = new Movie();
				mov.setId(row.getInt("MOVIE_ID"));
				mov.setYear(row.getInt("YEAR"));
				mov.setName(row.getString("MOVIE_NAME"));
				mov.setGenre1(row.getString("GENRE1"));
				mov.setGenre2(row.getString("GENRE2"));
				mov.setGenre3(row.getString("GENRE3"));
				System.out.println();
				movies.add(mov);
				counter++;
			}			
		}
		return movies;
	}
	
	public static void main(String args[]) {
		MovieRecommendationDAO mDAO = new MovieRecommendationDAO();
		mDAO.init();
		List<Movie> movies = mDAO.getMoviesFromGenre("Thriller");
		System.out.println(movies.size()+"|"+ counter);
		session.close();
	}
}
