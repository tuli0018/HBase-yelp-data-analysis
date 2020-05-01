import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import sun.lwawt.macosx.CPrinterDevice;

/**
 * @author ADVanced Team
 */
public class Ratings {	
	
	String city = "Tucson";
	
	/**
	 * List of locations to look up stats on
	 */
	private List<String> locations = Arrays.asList(
		"Las Vegas_NV",
		"Charlotte_NC",
		"Madison_WI",
		"Cleveland_OH",
		"Toronto_ON"
	);
	
	/**
	 * List of locations to look up stats on
	 */
	private List<String> users = Arrays.asList(
		"Chris",
		"Lisa",
		"Peter"
	);
	
	/**
	 * List of businesses to look up stats on
	 */
	private List<String> businesses = Arrays.asList(
		"Arby's",
		"Wendy's",
		"McDonald's",
		"Taco Bell"
	);
	

	/**
	 * prints business name that has more than 3 stars
	 * @param start 
	 * @throws IOException 
	 */
	public void StartScanReviwFilter(int review) throws IOException {
		String columnFamilyName = "reviewCount";
		Table table = HbaseMgr.GetInstance().GetTable("yelp","business_name");
		HbaseUtils.GetValueFromTable(review,columnFamilyName,"*",table);
	}
	
	/**
	 * prints business name that has more than 20 reviews
	 * @param start 
	 * @throws IOException 
	 */
	public void StartScanFilter(int star) throws IOException {
		String columnFamilyName = "rating";
		Table table = HbaseMgr.GetInstance().GetTable("yelp","business_location");
		HbaseUtils.GetValueFromTable(star,columnFamilyName,"rating",table);
	}
	
	//yelp:business_ratings
	/**
	 * Prints out the stats for users' reviews on businesses
	 */
	public void GetStatsReview() {
		this.users.forEach((String name) -> this.PrintUsersStats(name));		
	
	}
	
	/**
	 * Prints out the stats for ratings
	 */
	public void GetStats() {
		this.businesses.forEach((String name) -> this.PrintBusinessStats(name));		
		this.locations.forEach((String location) -> this.PrintLocationStats(location));
		
		// print out McDonald's ratings for each location
		for(String businessName : businesses) {
			this.locations.forEach((String location) -> this.PrintBusinessLocationStats(businessName, location));
		}
	}
	
	/**
	 * Prints stats for the Arby's ratings
	 * @throws IOException
	 */
	private void PrintBusinessStats(String businessName) {
		System.out.println(String.format("\nStats For Business: %1$s", businessName));
		String columnFamilyName = "rating";
		try {
			Result r = this.GetRatingsForBusiness(businessName);
			HbaseUtils.PrintStats(r, columnFamilyName);
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public void PrintCompareResults(String businessName1, String businessName2) throws Exception {
		System.out.println(String.format("Stats For Compare: %1$s", businessName1));
		try{
			Result r1 = this.GetRatingsForBusiness(businessName1);
			Result r2 = this.GetRatingsForBusiness(businessName2);
			HbaseUtils.compareResults(r1, r2);
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	
	/**
	 * Prints stats for the Users
	 * @throws IOException
	 */
	private void PrintUsersStats(String userName) {
		System.out.println(String.format("\nStats User: %1$s", userName));
		String columnFamilyName = "user_name";
		try {
			Result r = this.GetRatingsForUser(userName);
			HbaseUtils.PrintStats(r, columnFamilyName);
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	/**
	 * Prints stats for the location ratings
	 * @throws IOException
	 */
	private void PrintLocationStats(String locationName) {
		System.out.println(String.format("\nStats For Location: %1$s", locationName));
		String columnFamilyName = "rating";
		try {
			Result r = this.GetRatingsForLocation(locationName);
			HbaseUtils.PrintStats(r, columnFamilyName);
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Prints stats for the combination of location and business.
	 * @param businessName
	 * @param locationName
	 */
	private void PrintBusinessLocationStats(String businessName, String locationName) {		
		System.out.println(String.format("\nStats for %1$s in %2$s:", businessName, locationName));
		String columnFamilyName = "rating";
		try {
			// get the location ratings
			Result locationRatingsResult = this.GetRatingsForLocation(locationName);
			Set<Entry<byte[], byte[]>> locationRatingsSet = HbaseUtils.GetFamily(locationRatingsResult, columnFamilyName).entrySet();
			
			// get the business ratings
			Result businessRatingsResult = this.GetRatingsForBusiness(businessName);
			Set<Entry<byte[], byte[]>> businessRatingsSet = HbaseUtils.GetFamily(businessRatingsResult, columnFamilyName).entrySet();
			
			// get the intersection sets and get one of them (both contain ratings so it does not matter which)
			Set<Entry<byte[], byte[]>> intersection = HbaseUtils.GetIntersection(locationRatingsSet, businessRatingsSet).getLeft();
			
			// print the stats of the intersection (should be ratings)
			HbaseUtils.PrintStats(intersection);
		}
		catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Gets the ratings for the specified business name
	 * @param businessName
	 * @return
	 * @throws IOException
	 */
	private Result GetRatingsForBusiness(String businessName) throws IOException {
		Table table = HbaseMgr.GetInstance().GetTable("yelp", "business_name");
		return HbaseUtils.GetRowFromTable(String.format("\"%1$s\"", businessName), table);
	}	
	
	/**
	 * Gets the ratings for the specified location
	 * @param locationName
	 * @return
	 * @throws IOException
	 */
	private Result GetRatingsForLocation(String locationName) throws IOException {
		Table table = HbaseMgr.GetInstance().GetTable("yelp", "business_location");
		return HbaseUtils.GetRowFromTable(String.format("%1$s", locationName), table);
	}
	
	/**
	 * Gets the users
	 * @param userName
	 * @return
	 * @throws IOException
	 */
	private Result GetRatingsForUser(String userName) throws IOException {
		Table table = HbaseMgr.GetInstance().GetTable("yelp", "user_avgstarts");
		return HbaseUtils.GetRowFromTable(String.format("%1$s", userName), table);
	}
}
