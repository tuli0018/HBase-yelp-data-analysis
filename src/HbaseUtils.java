import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.hadoop.hbase.Cell;
import org.apache.commons.math3.*;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.jasper.tagplugins.jstl.ForEach;

/**
 * @author ADVanced Team
 */
/**
 * @author ahmed
 *
 */
public class HbaseUtils<res2> {
	/**
	 * Prints the average of the column family. 
	 * 
	 * Note: This function will fail on column families that do not have a numeric value.
	 * @param result
	 * @param columnFamilyName
	 */
	public static void PrintStats(Result result, String columnFamilyName) {
		Set<Entry<byte[], byte[]>> entrySet = GetFamily(result, columnFamilyName).entrySet();
		HbaseUtils.PrintStats(entrySet);
	}
	
	/**
	 * Prints the average of the column family as the entry set. 
	 * 
	 * Note: This function will fail on column families that do not have a numeric value.
	 * @param entrySet
	 */
	public static void PrintStats(Set<Entry<byte[], byte[]>> entrySet) {
		double size = entrySet.size();
		double totalRatings = 0.0;
		// set minimum to max value so we can overwrite it
		double minimum = Double.MAX_VALUE;
		// set maximum to min value so we can overwrite it
		double maximum = Double.MIN_VALUE;
		for (Entry<byte[], byte[]> entry : entrySet) {
			double rating = Double.parseDouble(Bytes.toString(entry.getValue()));
			totalRatings += rating;
			if (rating < minimum) {
				minimum = rating;
			}
			if (rating > maximum) {
				maximum = rating;
			}
		}

		List<Double> ratings_list = new ArrayList<>();
		for (Entry<byte[], byte[]> entry : entrySet) {
				double rating = Double.parseDouble(Bytes.toString(entry.getValue()));
			ratings_list.add(rating);
		}

		double[] ratings_array = ArrayUtils.toPrimitive(ratings_list.toArray(new Double[ratings_list.size()]));

		// get average to two decimal places
		double places = 2;
		double multiplier = Math.pow(10, places);
		double avg = Math.round((totalRatings / size) * multiplier) / multiplier;
		double median = median(ratings_array);
		double mean = mean(ratings_array);
		double lowerQuartile = quartile(ratings_array, 25);
		double upperQuartile = quartile(ratings_array, 75);
		
		// print the statistics for the family
		System.out.println(String.format("Average: %1$s", avg));
		System.out.println(String.format("Min: %1$s", minimum));
		System.out.println(String.format("Max: %1$s", maximum));
		System.out.println(String.format("Size: %1$s", size));
		System.out.println(String.format("Median: %1$s", median));
		System.out.println(String.format("Mean: %1$s", mean));
		System.out.println(String.format("Lower Quartile: %1$s", lowerQuartile));
		System.out.println(String.format("Upper Quartile: %1$s", upperQuartile));
	}
	
	/**
	 * Gets the size of the column family from the result.
	 * @param result
	 * @param columnFamilyName
	 * @return
	 */
	public static double GetColumnFamilySize(Result result, String columnFamilyName) {
		return result.getFamilyMap(Bytes.toBytes(columnFamilyName)).entrySet().size();
	}	
	
	/**
	 * Prints the column pairs based up to the rowCount given.
	 * @param result
	 * @param columnFamilyName
	 * @param rowCount
	 */
	public static void PrintColumnFamily(Result result, String columnFamilyName, int rowCount) {
	    int count = 0;	    
		Set<Entry<byte[], byte[]>> entrySet = GetFamily(result, columnFamilyName).entrySet();
		for (Entry<byte[], byte[]> entry : entrySet) {
			System.out.println(String.format("Key: %1$s, Value: %2$s"
											 , Bytes.toString(entry.getKey())
											 , Bytes.toString(entry.getValue())));
			if (++count >= rowCount) {
				break;
			}
		}
	}

	public static double quartile(double[] values, double lowerPercent) {
		DescriptiveStatistics da = new DescriptiveStatistics(values);
		return da.getPercentile(lowerPercent);
	}

	public static double median(double[] m) {
		Median median = new Median();
		return median.evaluate(m);
	}

	public static double mean(double[] m) {
		Mean mean = new Mean();
		return mean.evaluate(m);
	}

	
	/**
	 * Gets the family maps from a HBase database row (Result type).
	 * @param result
	 * @param columnFamilyName
	 * @return
	 */
	public static NavigableMap<byte[], byte[]> GetFamily(Result result, String columnFamilyName) {
		return result.getFamilyMap(Bytes.toBytes(columnFamilyName));
	}	
	
	/**
	 * Gets a specific family value from a column family
	 * @return
	 */
	public static byte[] GetRowFromFamily(NavigableMap<byte[], byte[]> columnFamily, String columnFamilyRowKey) {
		return columnFamily.get(Bytes.toBytes(columnFamilyRowKey));
	}
	
	/**
	 * Gets a row from an HBase table.
	 * 
	 * From this result, you can further get more information by 
	 * @param rowKey
	 * @param table
	 * @return
	 * @throws IOException
	 */
	public static Result GetRowFromTable(String rowKey, Table table) throws IOException {
	    // return the row that was requested from the table
	    return table.get(new Get(Bytes.toBytes(rowKey)));
	}	
	
	public static void GetValueFromTable(int rowValue, String columnFamily, String columnName, Table table) throws IOException {
	    // return value of row that was requested from the table
		Scan scan = new Scan();
		 SingleColumnValueFilter filterOne = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
				 Bytes.toBytes(columnName), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(rowValue));
		 scan.setFilter(filterOne);
		 ResultScanner scanner = table.getScanner(scan);
		 PrintFilterResult(scanner);
	}
	
	
	/**
	 * print the result of StartScanFilter function
	 * @param result
	 * @throws IOException
	 */
	private static void PrintFilterResult(ResultScanner result) throws IOException {
		int total = 0;
		Result resultBusiness = null;
		while(result.iterator().hasNext()) {
			resultBusiness = result.next();
			total++;
		 }
		System.out.println("Total number of businesses :" +total);
	}
	/**
	 * @param set1
	 * @param set2
	 * @return Key is the intersection with values from the first set, Value is the intersection with values from the second set.
	 */
	public static Pair<Set<Entry<byte[], byte[]>>, Set<Entry<byte[], byte[]>>> GetIntersection(Set<Entry<byte[], byte[]>> set1, Set<Entry<byte[], byte[]>> set2) {
		Set<Entry<byte[], byte[]>> intersection1 = new HashSet<Entry<byte[], byte[]>>();
		Set<Entry<byte[], byte[]>> intersection2 = new HashSet<Entry<byte[], byte[]>>();
		for (Entry<byte[], byte[]> entryFromSet1 : set1) {
			for (Entry<byte[], byte[]> entryFromSet2 : set2) {
				String key1 = Bytes.toString(entryFromSet1.getKey());
				String key2 = Bytes.toString(entryFromSet2.getKey());
				if (key1.equals(key2)) {
					intersection1.add(entryFromSet1);
					intersection2.add(entryFromSet2);
				}
			}
		}
		return Pair.of(intersection1, intersection2);
	}

	public static void compareResults(Result res1, Result res2) throws Exception {
		if (res2 == null) {
			throw new Exception("There wasn't enough rows, we stopped at "
					+ Bytes.toStringBinary(res1.getRow()));
		}
		if (res1.size() != res2.size()) {
			throw new Exception("This row doesn't have the same number of KVs: "
					+ res1.toString() + " compared to " + res2.toString());
		}
		Cell[] ourKVs = res1.rawCells();
		Cell[] replicatedKVs = res2.rawCells();
		for (int i = 0; i < res1.size(); i++) {
			if (!ourKVs[i].equals(replicatedKVs[i]) ||
					!Bytes.equals(CellUtil.cloneValue(ourKVs[i]), CellUtil.cloneValue(replicatedKVs[i]))) {
				throw new Exception("This result was different: "
						+ res1.toString() + " compared to " + res2.toString());
			}
		}
	}
}
