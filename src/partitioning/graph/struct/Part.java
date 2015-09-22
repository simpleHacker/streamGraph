package partitioning.graph.struct;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * vitual node object for hashing parition
 * 
 * @author AI
 */
public class Part {
		
	public String IP;
	public String port; 
	public String stmtfile;//stmtfile = IP+"_"+port
	public int stmt_size;
	public ArrayList<String> stmts;
	public HashSet<String> resource_list;
	
	public Part(){
		stmt_size = 0;
		resource_list = new HashSet<String>(10000,0.90f);
		stmts = new ArrayList<String>();
	}
	public String toString(){
		return IP+":"+port;
	}	
	
}
