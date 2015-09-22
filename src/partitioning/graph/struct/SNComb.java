package partitioning.graph.struct;

import java.util.HashSet;

/**  
 * related triple size and neighbor list combination
 * for a resource in index
 * 
 * @author AI
 *
 */
public class SNComb {
	public int dsize;
	public HashSet<String> nlist;
	
	public SNComb(int datasize){
		dsize = datasize;
		nlist = new HashSet<String>();
	}
}
