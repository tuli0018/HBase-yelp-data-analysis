import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

/**
 * @author ADVanced Team
 */
public class HbaseMgr {
	
	/**
	 * private instance for this singleton class
	 */
	private static HbaseMgr instance;	
	
	/**
	 * Connection to the Hbase database
	 */
	private Connection connection;	
	
	/**
	 * Administrator connection, which can perform admin level queries on the database.
	 */
	private Admin adminConnection;	

	/**
	 * Config file for the Hbase database.
	 */
	private Configuration config;
	
	/**
	 * Gets the configuration for the Hbase database
	 * @return
	 */
	private Configuration GetConfig() {
		Configuration config = HBaseConfiguration.create();
		String path = this.getClass().getClassLoader().getResource("HbaseConfig.xml").getPath();
		config.addResource(new Path(path));
		return config;
	}	
	
	/**
	 * Private constructor for singleton. Please call HbaseMgr.GetInstance() instead.
	 * @throws IOException
	 */
	private HbaseMgr() throws IOException {
		this.config = this.GetConfig();
		this.connection = ConnectionFactory.createConnection(this.config);
		this.adminConnection = this.connection.getAdmin();
	}
	
	/**
	 * Gets the singleton instance of the HbaseMgr.
	 * @return
	 * @throws IOException
	 */
	public static HbaseMgr GetInstance() throws IOException {
		if (instance == null) {
			instance = new HbaseMgr();
		}
		return instance;
	}	
	
	/**
	 * Cleans up the singleton instance
	 */
	public void Destroy() {
		try {
			this.connection.close();
			this.adminConnection.close();
		}
		catch (IOException ex) {
			// do nothing; it failed, can't really do anything about it
			ex.printStackTrace();
		}
		instance = null;
	}

	/**
	 * Gets a table from the hbase database.
	 * @param namespace
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public Table GetTable(String namespace, String tableName) throws IOException {
		// format the table name as NAMESPACE:TABLE_NAME
		String tableNameWithNamespace = String.format("%1$s:%2$s", namespace, tableName);
		
		// Create a table name object and query the table through the connection
		TableName tableNameProp = TableName.valueOf(tableNameWithNamespace);
	    return this.connection.getTable(tableNameProp);
	}
}

