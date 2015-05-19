package SPar.assignment;

import java.util.Hashtable;
import java.util.Set;

import SPar.struct.Pair;

public abstract class ParAlgo {
	Hashtable<Integer, Integer> cache;
	Hashtable<Integer,Integer> ages;
	int pre_part; // for random locality algorithm use to assign
	int slots = 4; // slot number can be decided by following method used for cache replacement design
	
// need to use sample sub dataset from dataset to get the good relax_f, role divide factor.	
	
	float relax_f;
	public ParAlgo(){
		cache = new Hashtable<Integer,Integer>(slots+1);
		ages = new Hashtable<Integer,Integer>(slots+1);
		pre_part = 0;
	}
// balance controller euqipped with random strategy	
	public abstract Pair<Integer,Integer> balanceP(int[] parts_stmt_info, float no_triples);
// random data distribiting strategy	
	public abstract void Adjust(int newpart, int[] parts_stmt_info, float no_triples);
	
	/**
	 * streaming partitioning: neighborhood greedy & size balanced
	 * 
	 * when res's neighbors in different parts form the same join number, we take the part which has less triple number
	 * this algorithm only work on history part, not open new part
	 * 
	 * @param res, resource for assignment
	 * @param sumlist, sumup neighbor info in each partition
	 * @param parts_stmt_info, statistic of number of stmt in each partition
	 * @param no_triples, expected total number of stmt in each parition
	 * @return
	 */
		public Pair<Integer,Integer> streamP(Hashtable<Integer,Integer> sumlist,int[] parts_stmt_info, float no_triples){
			int joins;
			float wi, max = 0,weight = -1;
			int maxJ = 0;
			int index = -1;
			if(sumlist != null){
				Set<Integer> parts = sumlist.keySet();
			
				for(int p : parts){
					joins = sumlist.get(p);
					wi = 1-parts_stmt_info[p]/no_triples;
					weight = joins*wi;
					
					if(weight < 0) continue;
					if(weight > max){
						max = joins*wi;
						index = p;
						maxJ = joins;
					}else{
						if(index == -1)
							continue;
						if (parts_stmt_info[p] >= no_triples)
							continue;
						if((weight == max) && (parts_stmt_info[p]<parts_stmt_info[index])){ // balance purpose
							max = weight;
							index = p;
							maxJ = joins;
						}
					}
				}
			}
		// no joins, balanced partitioning used	
			if(index == -1) return balanceP(parts_stmt_info, no_triples);
			else{
				Pair<Integer,Integer> selection = new Pair<Integer,Integer>();
				selection.part = index;
				selection.type = maxJ;
		//  for pre_part, only for LRU strategy
				Adjust(index,parts_stmt_info,no_triples);
				return selection;
			}
		}
		
		/** (optional - cost added)
		 * if there is a edge in str_buffer size has a weight larger than 1
		 * this function can help to optimize assignment also by consider weight of edges
		 * put info in sumlist
		 */	
			
//			public Pair<Integer,Float> streamP(String res, Hashtable<Integer,HashSet<String>> sumlist, Hashtable<String, HashSet<Node[]>> str_buffer,int[] parts_stmt_info, float no_triples){
				
//			}
}
