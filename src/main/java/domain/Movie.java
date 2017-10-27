package domain;

public class Movie {

	private long id;
	private long year;
	private String name;
	private String genre1;
	private String genre2;
	private String genre3;
	
	public Movie(long id, long year, String name, String genre1, String genre2, String genre3) {
		this.id = id;
		this.year = year;
		this.name = name;
		this.genre1 = genre1;
		this.genre2 = genre2;
		this.genre3 = genre3;
	}
	public Movie() {
		// TODO Auto-generated constructor stub
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public long getYear() {
		return year;
	}
	public void setYear(long year) {
		this.year = year;
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
	public String getGenre3() {
		return genre3;
	}
	public void setGenre3(String genre3) {
		this.genre3 = genre3;
	}
	
}
