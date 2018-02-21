package edu.tamu.nmp.domain;

/**
 * Model class for Ratings
 * 
 * @author team7
 *
 */
public class Ratings {

	private long movieId;
	private long userId;
	private int rating;
	private String Date;

	public Ratings(long movieId, long userId, int rating, String date) {
		super();
		this.movieId = movieId;
		this.userId = userId;
		this.rating = rating;
		Date = date;
	}

	public long getMovieId() {
		return movieId;
	}

	public void setMovieId(long movieId) {
		this.movieId = movieId;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public String getDate() {
		return Date;
	}

	public void setDate(String date) {
		Date = date;
	}

}
