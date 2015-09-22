package partitioning.graph.assignment;

import partitioning.graph.struct.Pair;

/**
 * balanced total random partitioning 
 * less good than LRU. better than block one only  
 * @author ray
 *
 */
public class BalanceGreedy extends ParAlgo{

	public Pair<Integer,Integer> balanceP(int[] parts_stmt_info, float no_triples){
			int min = parts_stmt_info[0];
			int index = -1;
			for(int i=1;i<parts_stmt_info.length;++i){
				if(parts_stmt_info[i] < min) {
					min = parts_stmt_info[i];
					index = i;
				}
			}
			Pair<Integer,Integer> p = new Pair<Integer,Integer>();
			if(index == -1){
				p.part = 0;
				p.type = 0; 
			}else{
				p.part = index;
				p.type = 0;
			}
			return p;
		}
		
		public void Adjust(int newpart, int[] parts_stmt_info, float no_triples){
			
		}
}
