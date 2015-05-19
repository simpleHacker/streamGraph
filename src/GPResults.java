import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import SPar.Retrival;


public class GPResults {
	public static void main(String[] args){
		
/**arguments
 * $0: parts number; $1: query base folder position; $2: partition folder position
 */
		Properties pro = new Properties();
		try{
			String filepath = args[1];
			pro.load(new FileInputStream(filepath+File.separator+"config.properties"));
			
			String queryPrefix = pro.getProperty("query_folder"); // query folder position
			String partitionPrefix = pro.getProperty("partition_folder"); // partition dataset folder position
			
			String queryfolder = queryPrefix+File.separator+"q"+args[0];//args[0]
			String gp_parfolder = partitionPrefix+File.separator+"GP_stmts_"+args[0];//args[0]
			//String cqueryfolder = "testpar";
			int parts = Integer.parseInt(args[0]);	// Integer.partInt(args[0]);
			
			
			String db = pro.getProperty("db_name");
			String dbtable = pro.getProperty("db_table");
			String port = pro.getProperty("db_port");
			
			Retrival rt = new Retrival(File.separator,parts,db,dbtable, port);
			
			int statistic;
	/*		  flag denote which statistic is carried on (GP-0;DHT-1)	
	*		  when flag=0 for DHT, means DHT use coordinater free design		
	*			
	*/
			ArrayList<String> querylist = rt.allquerys(queryfolder, parts); // regular query
//			ArrayList<String> querylist = rt.chainquerys(cqueryfolder);
			int flag = 0;
			statistic = rt.queryExecute(querylist,gp_parfolder,parts,flag);
			//remeber also add in progress info in it	 
			// if can also add join cost infomation	 
			System.out.println("GP Query message number in querying: "+statistic); 
			System.out.println("sweparator: "+File.separator);
//			 cear db table and close connection	
			rt.mysql.deleteAll();
			rt.mysql.closeConnection();
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}
	}
}
