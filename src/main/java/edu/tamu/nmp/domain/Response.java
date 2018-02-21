package edu.tamu.nmp.domain;

/**
 * Model classes for user response
 * 
 * @author team7
 */
public class Response {
	private long userId;
	private long year;
	private String genre;

	public Response(long userId, long year, String genre) {
		super();
		this.userId = userId;
		this.year = year;
		this.genre = genre;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public long getYear() {
		return year;
	}

	public void setYear(long year) {
		this.year = year;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

}
