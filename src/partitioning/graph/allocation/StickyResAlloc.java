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
 * this class allocate the stmt and resource to the assigned part by Palgorithm strictly
 * @author topwin
 *
 */
public class StickyResAlloc extends ResAlloc {
	public StickyResAlloc(String parfolder, String symbol, int parts, float tripleNo, DBOperation mysql) {
		super(parfolder, symbol, parts, tripleNo, mysql);
	}
	
/**
 * this function allocate the triple related with the res to assigned partition with no change
 * and update the index with new res and its neis info in assigned partition.
 * 
 */
	
	public int updateIndex(String res, Pair<Integer, Integer> assign, Hashtable<String, HashSet<Node[]>> str_buffer,
			Hashtable<String, Hashtable<String,Integer>> str_nei,Hashtable<String, Hashtable<Integer, Quad>> ind_buffer,
			int index_flag, int[] parts_stmt_info){
				HashSet<Node[]> trilist;  
				Node l;
				String sub, obj, pre;
				int ts, to, fts=0,fto=0;
				StringBuilder stmt;

				ArrayList<String> par =null;
				Hashtable<Integer, Quad> items=null;
				int s_len, o_len;

				trilist = str_buffer.get(res); 
				if(ind_buffer.containsKey(res))
					items = ind_buffer.get(res);

				int newneis = 0; // No. of new neis added in the assigned part index
				
			/**
			 *  with doning this, the content of str_buffer will not be changed for triple removal operation.
			 *  But extra space will be needed for removal operation to guarantee each stmt is processed once.
			 */			
				Set<Node[]> tempset = new HashSet<Node[]>();
				tempset.addAll(trilist);
				
				int s_sum = 0, o_sum = 0;
				
				for(Node[] node : tempset){	
					newneis++;
					
					ts=0; to=0;	
					s_len=0; o_len=0;
					sub = node[0].toN3();
					pre = node[1].toN3();
					obj = node[2].toN3();
					stmt = new StringBuilder(sub.length()+pre.length()+obj.length()+4);
					stmt.append(sub).append(" ").append(pre).append(" ").append(obj).append(" .");
					l = node[2];
					s_len = pre.length()+obj.length();
			// 1, allocate the triple to the assigned part strictly & put resource in that part index
					
					par = tp.get(assign.part);
					par.add(stmt.toString());
					
					parts_stmt_info[assign.part]++; // update part stmt info
					
					HashSet<Node[]> trip = str_buffer.get(sub);
					if(trip != null){ //shouldn't be null
						trip.remove(node); 
						str_buffer.put(sub, trip);
					}
	
					if(!(l instanceof Literal)){
						
						o_len = pre.length()+sub.length();
						
						if(sub.equals(obj)){ 
							ts = 1; to = 1;
							s_sum += s_len;
						}else{	
							trip = str_buffer.get(obj);
							if(trip != null){
								trip.remove(node); 
								str_buffer.put(obj, trip); 
							}
							
							if(res.equals(sub)) {
						/*		if(index_flag != 0)  update parts res info, can be done by query mysql after finished
									items2 = mysql.searchIndex(obj); // ???for sum parts_res_info[], or have better way to do it
								if(items2 == null || !items2.containsKey(assign.part))
									flag = 1; */
								
								ts = 1;
								s_sum += s_len; // sum res as sub's rest part of stmt bytes
								int upweight = 1;
								char type = 'o';
								updateRes(obj,type,upweight,assign.part,o_len,ind_buffer,parts_stmt_info[assign.part]);
								//updateRes(obj,type,upweight,assign.part,o_len,sed_ind,parts_stmt_info[assign.part]);
								
							}else {
							/*	if(index_flag != 0)
									items2 = mysql.searchIndex(sub); // ???for sum parts_res_info[], or have better way to do it
								if(items2 == null || !items2.containsKey(assign.part))
									flag = 1;*/
								
								to = 1;
								o_sum += o_len; // sum res as obj's rest part bytes
								int upweight = 1;
								char type = 's';
								updateRes(sub,type,upweight,assign.part,s_len,ind_buffer, parts_stmt_info[assign.part]);
							//	updateRes(sub,type,upweight,assign.part,s_len,sed_ind, parts_stmt_info[assign.part]);
							}
						/* if the nei of res is not in the assign part, update part res info
						 * flag check if the res in that part in buffer index or mysql index	
							if(flag == 1)
								parts_res_info[assign.part]++; */
							
							
				/*			if(!nres.equals(res)){
								updateNeis(str_nei, nres, res);
								updateNeis(str_nei, res, nres);
							}*/
						}
					} else{
						ts = 1;
						
					}
					fts |= ts;
					fto |= to;
					
				}// for
				
			/** allocate the res and update the index of it in assigned partition
			 in here, all neis have been allocated already, so only res need to add in assigned part;	*/
					
				Quad detail=null;
				if(items != null){
					if(items.containsKey(assign.part)){
						detail = items.get(assign.part);		
						detail.weight += newneis;
					}else{
						detail = new Quad();
						detail.weight = newneis;
					}
				}else{
					items = new Hashtable<Integer, Quad>();
					detail = new Quad();
					detail.weight = newneis;
				}
				detail.type_o |= fto;
				detail.type_s |= fts;
				detail.o_bytes += o_sum;
				detail.s_bytes += s_sum;
				
				items.put(assign.part, detail);
				ind_buffer.put(res, items);
				
	/************************************inspection checker*****************************
				items = null;
				detail = null;
				if(sed_ind.containsKey(res))
					items = sed_ind.get(res);
				if(items != null){
					if(items.containsKey(assign.part)){
						detail = items.get(assign.part);		
						detail.weight += newneis;
					}else{
						detail = new Quad();
						detail.weight = newneis;
					}
				}else{
					items = new Hashtable<Integer, Quad>();
					detail = new Quad();
					detail.weight = newneis;
				}
				detail.type_o |= fto;
				detail.type_s |= fts;
				detail.o_bytes += o_sum;
				detail.s_bytes += s_sum;
				
				items.put(assign.part, detail);
				sed_ind.put(res, items);
	/****************************************************************************/
				
			// update str_nei by remove allocated one		
			//	str_nei.remove(res);

			//  write out buffered stmt for that partition
				if(par != null && par.size() > sizelimit)
					writeoutTri(assign.part,par,parfolder);
			
			//  write out index buffer to mysql when is full.
				if(ind_buffer.size() >= INDEX_BUFFER_SIZE){
					mysql.createIndex(ind_buffer);
					
		/*******************inspection checker*********************
		//			showInd(ind_buffer);
					updateInd(ind_checker, ind_buffer);
					checkConsistency(ind_checker,sed_ind);
		/****************************************************/			
					ind_buffer.clear();
					
					return 1;
				}
				return 0;
	}
	
	public void checkInd(String nei, Hashtable<String, Hashtable<Integer, Quad>> ind_buffer){
		Hashtable<Integer, Quad> test = ind_buffer.get(nei);
		if(test != null){
			System.out.println("+++++++++++ind_buffer information for "+nei);
			for(int i: test.keySet()){
				System.out.println("part "+i+" Quad: "+test.get(i).toString());
			}
			System.out.println("---------------------------");
		}
			
	}

	public void updateNeis(Hashtable<String, Hashtable<String,Integer>> str_nei,String res, String nei){
		int count;
		Hashtable<String,Integer> neis = str_nei.get(res);
		if(neis != null)
			if(neis.keySet().size() != 0){
				count = neis.get(nei);
				if(count == 1)
					neis.remove(nei);
				else{
					count--;
					neis.put(nei, count);
				}
				if(neis.keySet().size() != 0)
					str_nei.put(res, neis);
				else
					str_nei.remove(res);
			}	
	}
}
