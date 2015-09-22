package partitioning.graph.allocation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

import partitioning.graph.struct.Pair;
import partitioning.graph.struct.Quad;
import partitioning.graph.utility.DBOperation;

/**
 * A Triple allocation algorithm, place RDF triples to a partition considering co-subject triple clustering.
 * Algorithm:
 * 1, the triple with the same sub should be placed into the same partition
 * 2, for random placement, if a partition's size is already access 3/4 full size, place it into different partition
 * if all partition size is over 3/4, just take the last allocated partition as assignment
 * 3, when a sub of triple already exist in a part as sub, whatever this sub assigned to, put this sub into the same part;
 * if only obj exist, and sub not exist (and any other case), when res==obj, assign it to assigned.part of algorithm; 
 * (when res != obj, put it to obj part with the largest weight; for the others, just random allocation) - put in res assignment algorithm
 * 4, for the other assignment, just do follow it
 * 5, Extreme case, if a res sub has so many triples to make the a part full, just assign it to the obj part, if there no 
 * obj part, place it to the sub part, 
 * 6, for a full size part, never random assign any triple to it, just remove it from the can be allocated part 
 * 7, con-sub assignment size cannot over threshold
 * 
 * @author ray
 *
 */

public class ConResAlloc extends ResAlloc{

	int lastpart;
	int shreshold;
	/**
	 * use dynamic proramming table princeple to optimize checing
	 * if res as sub of a stmt in hitorical index
	 * also need to update this time on the fly when assign a new sub res
	 * to a part, this is for next time fast lookup.
	 */
	Hashtable<String,Integer> subpart; // <res, as sub type part>
	
	public ConResAlloc(String parfolder, String symbol, int parts, float tripleNo, DBOperation mysql){
		super(parfolder, symbol, parts, tripleNo, mysql);
		shreshold = (int) (no_triples*3/4);
		subpart = new Hashtable<String,Integer>(); //give an estimate size for initial optimization
	}
	
	public void setThreshold(float value){
		threshold = value;
	}
	
/**
 * update index with new come res to a partition 
 * and allocate the triple with it into the partition.
 * 
 * @param res, resource updated in index
 * @param assign, the res assignment part by Palgorithm
 * @param str_buffer, the stmt buffer for stream in stmt
 * @param str_nei, neighbor list for stream in resources
 * @param ind_buffer, buffered index so far	
 * @param index_flag, flag to mark if on-disk index exist
 * @param parts_stmt_info, stmt number info of each part
 */	
	public int updateIndex(String res, Pair<Integer, Integer> assign, Hashtable<String, HashSet<Node[]>> str_buffer,
			Hashtable<String, Hashtable<String,Integer>> str_nei,Hashtable<String, Hashtable<Integer, Quad>> ind_buffer,
			int index_flag, int[] parts_stmt_info){
		HashSet<Node[]> trilist;  
		Node l;
		String sub, obj, pre;
		StringBuilder stmt;
		ArrayList<String> par =null;
		
	/*
	 * string buffer updates during triple placement
	 * 1, After assign all triples to one partition, need to remove those allocated triples from str_buff
	 * 2*, if want overlap, no need to remove 
	 */
		trilist = str_buffer.get(res);		
		
		Set<Node[]> tempset = new HashSet<Node[]>();
		tempset.addAll(trilist);
		
		Hashtable<Integer, Quad> reslist1=null, reslist2=null;
		int s_len, o_len;
		
		for(Node[] node : tempset){
			int finalp;
			s_len=0; o_len=0;
			sub = node[0].toN3();
			pre = node[1].toN3();
			obj = node[2].toN3();
			stmt = new StringBuilder(sub.length()+pre.length()+obj.length()+4);
			stmt.append(sub).append(" ").append(pre).append(" ").append(obj).append(" .");
			l = node[2];
			s_len = pre.length()+obj.length();
			
		/* 	allocate the triple to the right part & put resource in part's index
		 * 
		 *	check the sub part exist first, then obj part
		 *	for sub assignment, maybe against assigned algorithm
		 *	but for obj assignment, should follow the assigned algorithm 
		 */
			int unit = -1;
			if(subpart.containsKey(sub)){
				unit = subpart.get(sub);
			}else{
				if(ind_buffer.containsKey(sub)){
					reslist1 = ind_buffer.get(sub);
					unit = checksubpart('s', reslist1);
				}
				if(index_flag != 0 && unit == -1){
					reslist2 = mysql.searchIndex(sub);
					unit = checksubpart('s', reslist2);
				}
						
			/* check the index, if sub already exist as sub, place stmt in that part, 
			 *
			 * when res as obj of this stmt, but sub res never exsited (two options):
			 *  1, if the obj res already exsited as sub res in some part, place it in 
			 * 	2, choose the most frequent obj res part one to place the triple
			 * 
			 * if res as sub of stmt, but no res as sub before, place it into most frequent sub
			 * as obj part of stmt; another place it into most frequent res part for this obj of stmt
			 * -- no need to optimize here, no big advantage for that cost.
			 */
				subpart.put(sub, unit);
			}	
				
			int upweight;
			int parnum = -1;
			if(!(l instanceof Literal)){
					
				o_len = pre.length()+sub.length();
				if(unit != -1){
					parnum = parts_stmt_info[unit];
					if(parnum >= threshold) parnum = -1;
				}
				
				if(parnum != -1){ 
				// sub exist in index as sub type & part size < threshold, then allocate the triple
					par = tp.get(unit);
					par.add(stmt.toString());
					finalp = unit;
					
				// update the record of stmt info in partition	
					parts_stmt_info[unit]++;
					HashSet<Node[]> trip = str_buffer.get(sub);
					if(trip != null){
						trip.remove(node); 
						str_buffer.put(sub, trip);
					}

					trip = str_buffer.get(obj);
					
					if(trip != null){
						trip.remove(node); 
						str_buffer.put(obj, trip);
					}

				// allocate object resesource
				//	upweight = 1-parts_stmt_info[unit]/no_triples;
					upweight = 1; // one edge updates for two resource index
					updateRes(obj, 'o', upweight, unit, o_len, ind_buffer, parts_stmt_info[unit]);
					
				// allocate sub res
					updateRes(sub, 's', upweight, unit, s_len, ind_buffer, parts_stmt_info[unit]);

				} else { // sub not exist in hitorical index || part size >= threshold
				/* sub of the stmt not exsit as sub res in the index, assign to assigned part following Palgorithm*/
					
		/*
		 * here need to pay attention, the assign algorithm
		 * should take account of the size balance things, like not access the 3/4 of 
		 * full size !!!			
		 */
					
	    /*==================assignment algorithm working part====================					
		 * res == sub not exist in index, use assignment from assign algorithm; check obj
		 * assign algorithm chose the most neis one, or but choose most frequent obj part
		 * if assign.weight > 0 , should put in most neis part, else sub & obj are all
		 * not in index, no need to check mergelist again.	
		 * -- no need to optimize here, not worth for this little optimization			
		 *
		 * assignment choice assign.weight choice
		 * if neis distributed on several part and distribution is even, 
		 * the neis of a part has more weight will be chosen out;
		 * else if all equal, only chose the least size of part one 
		 * 	as assignment
		 */	
			// allocate the triple 
					par = tp.get(assign.part);
					par.add(stmt.toString());		
					finalp = assign.part;
			// update the record of stmt info in partition	
					parts_stmt_info[assign.part]++;
					HashSet<Node[]> trip = str_buffer.get(sub);
					if(trip != null){
						trip.remove(node); 
						str_buffer.put(sub, trip);
					}
					
					trip = str_buffer.get(obj);
					if(trip != null){
						trip.remove(node); 
						str_buffer.put(obj, trip);
					}
						
				// allocate sub res first
				//	upweight = 1-parts_stmt_info[assign.part]/no_triples;
					upweight = 1;
					updateRes(sub, 's', upweight, assign.part, s_len, ind_buffer, parts_stmt_info[assign.part]);
					
				// update DP table too for next time lookup	
					subpart.put(sub, assign.part);
					
				// allocate obj res second
					updateRes(obj, 'o', upweight, assign.part, o_len, ind_buffer, parts_stmt_info[assign.part]);
				}
			} else{ // obj as string case
			/* allocate the triple
			 * and allocate the sub res to the part
			 * also update the weight of res in that part
			 * only no needs to update the s_bytes and o_bytes info here,
			 * because those info is only for chain 2 query use for all resource stmts
			 */
				int part;
				
				if(unit != -1){
					parnum = parts_stmt_info[unit];
					if(parnum >= threshold) parnum = -1;
				}
				
				if(parnum != -1){
					par = tp.get(unit);
					par.add(stmt.toString());
					part = unit;
					
				} else{
					par = tp.get(assign.part);
					par.add(stmt.toString());	
					part = assign.part;		
				//	 update DP table too for next time lookup	
					subpart.put(sub, assign.part);
				}
				finalp = part;
			//	allocate sub res
			//	upweight = 1-parts_stmt_info[part]/no_triples;
				upweight = 1;
				updateRes(sub, 's', upweight, part, 0, ind_buffer, parts_stmt_info[part]);
				
			//	update the record of stmt info in partition	
				parts_stmt_info[part]++;
				HashSet<Node[]> trip = str_buffer.get(sub);
				if(trip != null){
					trip.remove(node); 
					str_buffer.put(sub, trip);
				}	
			}// end of resource and stmt assignment
			
			// check par size for batch writeout		
			if(par != null)
				if(par.size() > sizelimit){
					writeoutTri(finalp,par,parfolder);
				}
		}// for
		
	//  writeout index buffer to mysql when is full.
		if(ind_buffer.size() >= INDEX_BUFFER_SIZE){
			mysql.createIndex(ind_buffer);
			ind_buffer.clear();
			return 1;
		}
		return 0;
	}
	
	
/**
 * check if a res exist as 'type' in either parlist1 or 2, if exist, immediately return
 * check sub type first, if not, check obj type second with max weight one
 * @param type
 * @param parlist1
 * @param parlist2
 * @return
 */ 
	public int checksubpart(char type, Hashtable<Integer, Quad> parlist){
		int types = 0;
		Integer unit = -1;
		
		if(parlist == null) return unit;
		Set<Integer> keys = parlist.keySet();
		Quad pr;	
		for(int part : keys){
			pr = parlist.get(part);
			
			if(pr != null)
				types = pr.type_s; // type
			if(type == 's'){ 
				if(types != 0){	
					unit = part;
					return unit;
				}
			}
		}
		return unit;
	}
}
