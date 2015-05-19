import java.util.ArrayList;

import SPar.Retrival;


public class HResults {
	public static void main(String[] args){
		
		/**arguments
		 * $0: parts number; $1: query base folder position; $2: partition folder position
		 */
		
		String queryPrefix = args[1]; // query folder position
		String partitionPrefix = args[2]; // partition dataset folder position
		
		String queryfolder = queryPrefix+"q"+args[0];//args[0]
		String dht_parfolder = partitionPrefix+"DHT_stmts_"+args[0];
		//String cqueryfolder = "testpar";
		int parts = Integer.parseInt(args[0]);	// Integer.partInt(args[0]);
		
		String db = "gindex";
		String dbtable = "sindex";
		Retrival rt = new Retrival("/",parts,db,dbtable);
		
		int statistic;
/**		  flag denote which statistic is carried on (GP-0;DHT-1)	
*		  when flag=0 for DHT, means DHT use coordinater free design		
*			
*/
		ArrayList<String> querylist = rt.allquerys(queryfolder, parts); // regular query
//		ArrayList<String> querylist = rt.chainquerys(cqueryfolder);
		int flag = 1;
		statistic = rt.queryExecute(querylist, dht_parfolder, parts,flag);
		System.out.println("DHT Query message number in querying: "+statistic);		
		
		// cear db table and close connection	
		rt.mysql.deleteAll();
		rt.mysql.closeConnection();
	}
}
