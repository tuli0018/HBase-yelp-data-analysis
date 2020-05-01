import java.io.IOException;

/**
 * @author ADVanced Team
 */
public class Program {

	public void Start() throws Exception {
		Ratings ratings = new Ratings();
		ratings.GetStats();

		ratings.StartScanFilter(5);//return businesses that have more than 4 stars
		ratings.StartScanReviwFilter(5000);//return businesses that have more than 20 reviews


	}	
	
	public static void main(String[] args) throws Exception {
		Program program = new Program();

		System.out.println("Program Start");
		try {
			program.Start();
			HbaseMgr.GetInstance().Destroy();
		}
		catch  (IOException ex) {
			ex.printStackTrace();
		}
		System.out.println("Program End");
	}
}
