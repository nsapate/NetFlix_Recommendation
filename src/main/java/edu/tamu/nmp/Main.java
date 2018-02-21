package edu.tamu.nmp;

import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import edu.tamu.nmp.domain.Movie;
import edu.tamu.nmp.domain.User;
import edu.tamu.nmp.persistance.CassandraDataLoader;
import edu.tamu.nmp.persistance.MovieRecommendationDAO;
import edu.tamu.nmp.persistance.NewUserDAO;
import edu.tamu.nmp.persistance.UserPreferenceDAO;

/**
 * Main Class for netflix movie recommendation.
 * @author team 7
 *
 */
public class Main {
	private static String consoleInput = "";

	public static void main(String[] args) {
		CassandraDataLoader cdl = new CassandraDataLoader();
		cdl.runAll();
		UserPreferenceDAO userPref = new UserPreferenceDAO();
		MovieRecommendationDAO movieRec = new MovieRecommendationDAO();
		Scanner scanner = new Scanner(System.in);
		int flag = 1;
		while (true) {

			System.out.println("\nWelcome to Movie Recommendation System  ");
			System.out.println("Type");
			System.out.println("[Login] to enter the Recommendation System ");
			System.out.println("[Register] to create new Account ");
			System.out.println("[Help] to know more ");
			System.out.println("[Quit] to exit");
			System.out.println("\nInput : ");
			consoleInput = scanner.nextLine();

			if ("register".equalsIgnoreCase(consoleInput)) {
				User user = new User();
				NewUserDAO createUser = new NewUserDAO();
				Random rand = new Random();
				System.out.println("Welcome Guest. Let's create an account for you!");
				System.out.println("Enter your Name : ");
				user.setName(scanner.nextLine());
				System.out.println("What type of Movies do you like to watch  : ");
				user.setGenre1(scanner.nextLine());
				System.out.println("Tell us another genre of movies you would love watching :");
				user.setUser_id(Long.valueOf(rand.nextInt(900000) + 100000));
				user.setGenre2(scanner.nextLine());
				System.out.println("Tell us your preferred movie ratings : [Scale 1 to 5]");
				user.setRating(new Integer(scanner.nextLine()));
				System.out.println("Registration successfully completed \n");
				System.out.println("Hi " + user.getName() + " Your user Id is " + user.getUser_id());
				System.out.println("Use this ID for login!");
				createUser.insertNewUser(user);
			} else if ("login".equalsIgnoreCase(consoleInput)) {
				System.out.println(
						"\nPlease enter your ID: [Hint use id - 123456 or 56565  OR the new user ID generated]");
				consoleInput = scanner.nextLine();
				System.out.println("\nHere are some movies you can watch\n\n");
				if (consoleInput.isEmpty()) {
					System.out.println(
							"Error - Please enter your Correct User ID [Hint use existing id - 123456 or 56565  OR the new user ID generated]");
				} else {
					flag = userPref.getMovieRecommendationHstry(consoleInput);
					
					if (flag == -1)
						continue;
					System.out.println(
							"\n What type of Movie would you like to Watch today? : [Horror, Drama, Thriller, Action, Crime, Comedy etc]");
					System.out.println("Input:");
					consoleInput = scanner.nextLine();
					List<Movie> movies = movieRec.getMoviesFromGenre(consoleInput.toLowerCase());
					System.out.println("Here are couple of movies you can watch today : \n");
					int i = 1;
					for (Movie movie : movies) {
						if (i < 6) {
							System.out.println("--------------------------------------------------");
							System.out.println("  Name           |  " + movie.getName());
							System.out.println("  Year           |  " + movie.getYear());
							System.out.println("--------------------------------------------------");
							i++;
						} else {
							System.out.println();
							break;
						}
					}
				}
			} else if ("quit".equalsIgnoreCase(consoleInput)) {
				System.out.println("GoodBye!");
				break;
			} else if ("".equalsIgnoreCase(consoleInput)) {
				System.out.println("Please enter a value\n");
				continue;
			} else if ("help".equalsIgnoreCase(consoleInput)) {
				System.out.println("This application is a movie recommendation system which ");
				System.out.println("uses historical data of members and their rating to recommend movies");
				System.out.println("-Build by Team 7\n\n");
				continue;
			}
		}

		scanner.close();

	}

}
