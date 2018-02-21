package edu.tamu.nmp.domain;

/**
 * Model classes for User
 * 
 * @author team7
 */
public class User {

	private long user_id;

	private String name;

	private String genre1;

	private String genre2;
	
	private int rating;

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}

	public long getUser_id() {
		return user_id;
	}

	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGenre1() {
		return genre1;
	}

	public void setGenre1(String genre1) {
		this.genre1 = genre1;
	}

	public String getGenre2() {
		return genre2;
	}

	public void setGenre2(String genre2) {
		this.genre2 = genre2;
	}

	public User(long user_id, String name, String genre1, String genre2, int rating) {
		super();
		this.user_id = user_id;
		this.name = name;
		this.genre1 = genre1;
		this.genre2 = genre2;
		this.rating = rating;
	}

	public User() {
	}

}
