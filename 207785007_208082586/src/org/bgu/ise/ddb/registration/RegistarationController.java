/**
 * 
 */
package org.bgu.ise.ddb.registration;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoClients;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController {

	/**
	 * The function checks if the username exist, in case of positive answer
	 * HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT, else
	 * insert the user to the system and set to HttpStatus in HttpServletResponse
	 * HttpStatus.OK
	 * 
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method = { RequestMethod.POST })
	public void registerNewUser(@RequestParam("username") String username, @RequestParam("password") String password,
			@RequestParam("firstName") String firstName, @RequestParam("lastName") String lastName,
			HttpServletResponse response) {
		System.out.println(username + " " + password + " " + lastName + " " + firstName);
		// :TODO your implementation

		HttpStatus status = null;
		MongoClient client = null;
		
		try {
			if (isExistUser(username)) {
				status = HttpStatus.CONFLICT;
			} else {

				// creating a mongoDB client
				 client = new MongoClient("localhost", 27017);

				// retrieving the "User" table
				DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");

				BasicDBObject userObject = new BasicDBObject();
				userObject.put("username", username);
				userObject.put("password", password);
				userObject.put("firstName", firstName);
				userObject.put("lastName", lastName);
				userObject.put("registrationDate", new Date());

				userTable.insert(userObject);
				status = HttpStatus.OK;

				
			}
		} catch (Exception e) {
			System.out.println(e);
			status = HttpStatus.CONFLICT;
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		response.setStatus(status.value());

	}

	/**
	 * The function returns true if the received username exist in the system
	 * otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method = { RequestMethod.GET })
	public boolean isExistUser(@RequestParam("username") String username) throws IOException {
		System.out.println(username);
		boolean result = false;
		// :TODO your implementation
		
		MongoClient client = null;

		try {
			
			// creating a mongoDB client
			 client = new MongoClient("localhost", 27017);

			// retrieving the "User" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");

			BasicDBObject query = new BasicDBObject();
			query.put("username", username);
			DBCursor cursor = userTable.find(query);
			if (cursor.hasNext())
				result = true;


		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		return result;

	}

	/**
	 * The function returns true if the received username and password match a
	 * system storage entry, otherwise false
	 * 
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method = { RequestMethod.POST })
	public boolean validateUser(@RequestParam("username") String username, @RequestParam("password") String password)
			throws IOException {
		System.out.println(username + " " + password);
		boolean result = false;
		// :TODO your implementation
		
		MongoClient client = null;

		try {
			
			// creating a mongoDB client
			client = new MongoClient("localhost", 27017);

			// retrieving the "User" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");

			BasicDBObject query = new BasicDBObject();
			query.put("username", username);
			query.put("password", password);
			DBCursor cursor = userTable.find(query);
			if (cursor.hasNext())
				result = true;

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * 
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method = { RequestMethod.GET })
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException {
		System.out.println(days + "");
		int result = 0;
		// :TODO your implementation
		
		MongoClient client = null;
		
		try {

			// creating a mongoDB client
			client = new MongoClient("localhost", 27017);

			// retrieving the "User" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");

			Instant now = Instant.now();
			Instant wantedInstant = now.minus(days, ChronoUnit.DAYS);
			Date wantedDate = Date.from(wantedInstant);

			DBCursor cursor = userTable.find();
			while (cursor.hasNext()) {
				DBObject userObject = cursor.next();
				Date userRegistrationDate = (Date) userObject.get("registrationDate");
				if (userRegistrationDate.getTime() > wantedDate.getTime())
					result++;
			}

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		return result;

	}

	/**
	 * The function retrieves all the users
	 * 
	 * @return
	 */
	@RequestMapping(value = "get_all_users", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public User[] getAllUsers() {
		// :TODO your implementation
		// User u = new User("alex", "alex", "alex");
		// System.out.println(u);
		// return new User[]{u};

		User[] users = new User[0];
		List<User> userList = new ArrayList<>();
		
		MongoClient client = null;

		try {
			// creating a mongoDB client
			 client = new MongoClient("localhost", 27017);

			// retrieving the "User" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");

			DBCursor cursor = userTable.find();
			while (cursor.hasNext()) {
				DBObject userObject = cursor.next();
				String username = (String) userObject.get("username");
				String password = (String) userObject.get("password");
				String firstName = (String) userObject.get("firstName");
				String lastName = (String) userObject.get("lastName");
				User user = new User(username, password, firstName, lastName);
				userList.add(user);
			}
			
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		
		if (userList.size() > 0) {
			users = new User[userList.size()];
			userList.toArray(users);
		} 

		return users;

	}

}
