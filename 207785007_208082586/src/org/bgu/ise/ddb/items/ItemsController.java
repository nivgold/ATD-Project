/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.history.HistoryPair;
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

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {

	/**
	 * The function copy all the items(title and production year) from the Oracle
	 * table MediaItems to the System storage. The Oracle table and data should be
	 * used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method = { RequestMethod.GET })
	public void fillMediaItems(HttpServletResponse response) {
		// :TODO your implementation

		Connection oracleConnection = null;
		Statement statement = null;

		HttpStatus status = HttpStatus.OK;

		try {
			oracleConnection = createConnection();
			String query = "SELECT title, prod_year FROM MediaItems";
			statement = oracleConnection.createStatement();

			ResultSet mediaItemsIterator = statement.executeQuery(query);
			while (mediaItemsIterator.next()) {
				String title = mediaItemsIterator.getString(1);
				String prod_year = mediaItemsIterator.getString(2);

				// inserting to system storage
				insertMediaItemToSystem(title, prod_year);
			}

		} catch (Exception e) {
			System.out.println(e);
			status = HttpStatus.CONFLICT;
		} finally {
			// closing resources
			try {
				if (statement != null)
					statement.close();
				if (oracleConnection != null) {
					oracleConnection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
				status = HttpStatus.CONFLICT;
			}
		}

		response.setStatus(status.value());
	}

	/**
	 * The function copy all the items from the remote file, the remote file have
	 * the same structure as the films file from the previous assignment. You can
	 * assume that the address protocol is http
	 * 
	 * @throws IOException
	 */
	@RequestMapping(value = "fill_media_items_from_url", method = { RequestMethod.GET })
	public void fillMediaItemsFromUrl(@RequestParam("url") String urladdress, HttpServletResponse response)
			throws IOException {
		System.out.println(urladdress);

		// :TODO your implementation

		HttpStatus status = HttpStatus.OK;

		try (BufferedReader br = new BufferedReader(new InputStreamReader((new URL(urladdress).openStream())))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");

				String title = values[0];
				String prod_year = values[1];

				// inserting to the system storage
				insertMediaItemToSystem(title, prod_year);
			}
		} catch (IOException e) {
			e.printStackTrace();
			status = HttpStatus.CONFLICT;
		}

		response.setStatus(status.value());
	}

	/**
	 * The function retrieves from the system storage N items, order is not
	 * important( any N items)
	 * 
	 * @param topN
	 *            - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items", headers = "Accept=*/*", method = {
			RequestMethod.GET }, produces = "application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public MediaItems[] getTopNItems(@RequestParam("topn") int topN) {
		// :TODO your implementation
		// MediaItems m = new MediaItems("Game of Thrones", 2011);
		// System.out.println(m);
		// return new MediaItems[]{m};

		MediaItems[] mediaItems = new MediaItems[0];
		List<MediaItems> mediaItemsList = new ArrayList<>();

		if (topN <= 0) {
			return mediaItems;
		}

		MongoClient client = null;
		try {

			// creating a mongoDB client
			client = new MongoClient("localhost", 27017);

			// retrieving the "MediaItem" table
			DBCollection mediaItemTable = client.getDB("ATD_Project_DB").getCollection("MediaItem");

			// retrieving N items from MediaItem table
			DBCursor cursor = mediaItemTable.find().limit(topN);

			while (cursor.hasNext()) {
				DBObject mediaItem = cursor.next();
				mediaItemsList.add(new MediaItems((String) mediaItem.get("title"),
						Integer.parseInt((String) mediaItem.get("prod_year"))));
			}

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		if (mediaItemsList.size() > 0) {
			mediaItems = new MediaItems[mediaItemsList.size()];
			mediaItemsList.toArray(mediaItems);
		}

		return mediaItems;
	}

	/**
	 * create a connection to the ORACLE DB
	 * 
	 * @return a JDBC Connection
	 */
	private Connection createConnection() {
		String connectionString = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
		String DBUsername = "hila5";
		String DBPassword = "abcd";
		Connection connction = null;
		// Registering the oracle driver
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			connction = DriverManager.getConnection(connectionString, DBUsername, DBPassword);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return connction;
	}

	private void insertMediaItemToSystem(String title, String prod_year) {

		MongoClient client = null;

		try {
			// check if title is already in the system storage
			if (!checkIfTitleExist(title)) {

				// creating a mongoDB client
				client = new MongoClient("localhost", 27017);

				// retrieving the "MediaItem" table
				DBCollection mediaItemTable = client.getDB("ATD_Project_DB").getCollection("MediaItem");

				BasicDBObject mediaItemObject = new BasicDBObject();
				mediaItemObject.put("title", title);
				mediaItemObject.put("prod_year", prod_year);

				// inserting to the MediaItem table
				mediaItemTable.insert(mediaItemObject);
			}

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * check if the given title exists in the MediaItem collection system storage
	 * 
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

		} catch (Exception e) {
			System.out.println(e);
		} finally {
			// closing resources
			if (client != null) {
				client.close();
			}
		}

		return exist;
	}
}
