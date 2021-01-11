/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.model.DBCollectionFindAndModifyOptions;


/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	
	/**
	 * check if the given username exists in the User collection system storage
	 * @param username
	 * @return True if exists end False otherwise
	 */
	private boolean checkIfUsernameExist(String username) {
		boolean exist = false;
		
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
				exist = true;
			
			// closing cursor
			if (cursor != null) {
				cursor.close();
			}
			
		} catch(Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		return exist;
	}
	
	/**
	 * check if the given title exists in the MediaItem collection system storage
	 * @param title
	 * @return True if exists end False otherwise
	 */
	private boolean checkIfTitleExist(String title) {
		boolean exist = false;
		
		MongoClient client = null;
		
		try {
			
			// creating a mongoDB client
			client = new MongoClient("localhost", 27017);
						
			// retrieving the "MediaItem" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("MediaItem");
			
			BasicDBObject query = new BasicDBObject();
			query.put("title", title);
			DBCursor cursor = userTable.find(query);
			if (cursor.hasNext())
				exist = true;
			
			// closing cursor
			if (cursor != null) {
				cursor.close();
			}
			
		} catch(Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		return exist;
	}
	
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		System.out.println(username+" "+title);
		//:TODO your implementation
		boolean isInserted = false;
		
		MongoClient client = null;
		
		try {
			
			if (checkIfUsernameExist(username) && checkIfTitleExist(title)) {
				
				// creating a mongoDB client
				client = new MongoClient("localhost", 27017);
				
				// retrieving the "History" table
				DBCollection historyTable = client.getDB("ATD_Project_DB").getCollection("History");
				
				// getting the timestamp in ms since 1970
				long timestamp = Instant.now().toEpochMilli();
				
				BasicDBObject historyObject = new BasicDBObject();
				historyObject.put("username", username);
				historyObject.put("title", title);
				historyObject.put("timestamp", timestamp);
				
				historyTable.insert(historyObject);
				
				// changing flag to be true
				isInserted = true;
				
			}

		} catch(Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		HttpStatus status;
		if (isInserted)
			status = HttpStatus.OK;
		else
			status = HttpStatus.CONFLICT;
		
		response.setStatus(status.value());
	}
	
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
//		HistoryPair hp = new HistoryPair("aa", new Date());
//		System.out.println("ByUser "+hp);
//		return new HistoryPair[]{hp};
		
		HistoryPair[] userHistories = new HistoryPair[0];
		List<HistoryPair> userHistoriesList = new ArrayList<>();
		
		MongoClient client = null;
		
		try {
			if (checkIfUsernameExist(username)) {

				// creating a mongoDB client
				 client = new MongoClient("localhost", 27017);
				
				// retrieving the "History" table
				DBCollection historyTable = client.getDB("ATD_Project_DB").getCollection("History");
				
				BasicDBObject query = new BasicDBObject();
				query.put("username", username);
				
				// getting all of the users' history by descending order
				DBCursor queryResult = historyTable.find(query).sort(new BasicDBObject("timestamp", -1));
				
				
				// iterating through the histories
				while(queryResult.hasNext()) {
					DBObject history = queryResult.next();
					String title = (String) history.get("title");
					long timestamp = (long) history.get("timestamp");
					HistoryPair pair = new HistoryPair(title, new Date(timestamp));
					userHistoriesList.add(pair);
				}
				
				// closing cursor
				if (queryResult != null) {
					queryResult.close();
				}
				
			}
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		if (userHistoriesList.size() > 0) {
			userHistories = new HistoryPair[userHistoriesList.size()];
			userHistoriesList.toArray(userHistories);
		}
		
		return userHistories;

	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
//		HistoryPair hp = new HistoryPair("aa", new Date());
//		System.out.println("ByItem "+hp);
//		return new HistoryPair[]{hp};
		
		HistoryPair[] titleHistories = new HistoryPair[0];
		List<HistoryPair> titleHistoriesList = new ArrayList<>();
		
		MongoClient client = null;
		
		try {
			if (checkIfTitleExist(title)) {

				// creating a mongoDB client
				client = new MongoClient("localhost", 27017);
				
				// retrieving the "History" table
				DBCollection historyTable = client.getDB("ATD_Project_DB").getCollection("History");
				
				BasicDBObject query = new BasicDBObject();
				query.put("title", title);
				
				// getting all of the titles' history by descending order
				DBCursor queryResult = historyTable.find(query).sort(new BasicDBObject("timestamp", -1));
				
				// iterating through the histories
				while(queryResult.hasNext()) {
					DBObject history = queryResult.next();
					String username = (String) history.get("username");
					long timestamp = (long) history.get("timestamp");
					HistoryPair pair = new HistoryPair(username, new Date(timestamp));
					titleHistoriesList.add(pair);
				}
				
				// closing cursor
				if (queryResult != null) {
					queryResult.close();
				}
				
			}
			
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		if (titleHistoriesList.size() > 0) {
			titleHistories = new HistoryPair[titleHistoriesList.size()];
			titleHistoriesList.toArray(titleHistories);
		}
		
		return titleHistories;
	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
//		User hp = new User("aa","aa","aa");
//		System.out.println(hp);
//		return new User[]{hp};
		
		User[] users = new User[0];
		Set<User> userSet = new HashSet<>();
		
		HistoryPair[] titleHistories = this.getHistoryByItems(title);
		
		MongoClient client = null;
		
		if (titleHistories.length == 0) {
			return users;
		}
		
		try {
			
			// creating a mongoDB client
			client = new MongoClient("localhost", 27017);
			
			// retrieving the "User" table
			DBCollection userTable = client.getDB("ATD_Project_DB").getCollection("User");
			
			// iterating through the histories related to the given title
			for (HistoryPair history : titleHistories) {
				String username = history.getCredentials();
				
				// finding the User whose username is `username`
				BasicDBObject query = new BasicDBObject();
				query.put("username", username);
				
				DBCursor cursor = userTable.find(query);
				if (cursor.hasNext()) {
					DBObject user = cursor.next();
					userSet.add(new User((String)user.get("username"), (String)user.get("firstName"), (String)user.get("lastName")));
				}
				
				// closing cursor
				if (cursor != null) {
					cursor.close();
				}
				
			}
			
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
		
		if (userSet.size() > 0) {
			users = new User[userSet.size()];
			userSet.toArray(titleHistories);
		}
		
		return users;
	}
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		//:TODO your implementation
//		double ret = 0.0;
//		return ret;
		
		double similarity = 0;
		
		HistoryPair[] title1Histories = getHistoryByItems(title1);
		HistoryPair[] title2Histories = getHistoryByItems(title2);
		
		// converting to unique usernames sets
		Set<String> title1Set = new HashSet<>();
		for (HistoryPair pair : title1Histories) {
			title1Set.add(pair.getCredentials());
		}
		
		Set<String> title2Set = new HashSet<>();
		for (HistoryPair pair : title2Histories) {
			title2Set.add(pair.getCredentials());
		}
		
		// calculating the union set
		Set<String> unionUsers = new HashSet<>(title1Set);
		unionUsers.addAll(title2Set);
		
		// calculating the intersection
		Set<String> intersectionUsers = new HashSet<>(title1Set);
		intersectionUsers.retainAll(title2Set);
		
		// checking zero division
		if (unionUsers.size() == 0)
			similarity = 0;
		else {
			similarity = (intersectionUsers.size() / (double) unionUsers.size());
		}
		
		
		return similarity;
	}
	

}
