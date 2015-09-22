package partitioning.graph.allocation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

import partitioning.graph.struct.Pair;
import partitioning.graph.struct.Quad;

/*
 * p(i) should be the number of statements in that partition; C should be (Total number of statement)/(number of partitions)
 * 
 */

public class TrendyResAlloc extends ResAlloc {
	public TrendyResAlloc(String parfolder, String symbol, int parts, float tripleNo, DBOperation mysql) {
		super(parfolder, symbol, parts, tripleNo, mysql);
	}

	public void updateIndex(String res, Pair<Integer, Float> assign, Hashtable<String, HashSet<Node[]>> str_buffer,
			Hashtable<String, HashSet<String>> str_nei,Hashtable<String, Hashtable<Integer, Quad>> ind_buffer,
			int index_flag, int[] parts_res_info, int[] parts_stmt_info){
				HashSet<Node[]> trilist;  
				Node l;
				String sub, obj, pre, type, ts="", to="";
				StringBuilder stmt;
				ArrayList<Pair<Integer, String>> indlist;
				Pair<Integer,String> pp;
				ArrayList<String> par =null, spar = null;
				Hashtable<Integer,Quad> parlist;
		/*
		 * 1, After assign all triples to one partition, need to remove those allocated triples from str_buff
		 * 2*, if want overlap, no need to remove 
		 */
				trilist = str_buffer.get(res);
				int flag; // insert judge (0: this part; 1:other part)
				Hashtable<Integer, Quad> items=null;
				if(ind_buffer.containsKey(res))
					items = ind_buffer.get(res);
				String finalty = null; // res assigned part type
				int newneis = 0; // No. of new neis added in the assigned part index
			
				Set<Node[]> tempset = new HashSet<Node[]>();
				tempset.addAll(trilist);
				
				int s_len, o_len=0, s_sum_len = 0, o_sum_len = 0;
				
				for(Node[] node : tempset){	
				// decide the real assign part by checking all index options before everything
					Float wmax = 0f;
					int sig1 = -1;
					
					flag = 0;
					ts=""; to="";
				// check each one, then remove each one from the others.	
					
					sub = node[0].toN3();
					pre = node[1].toN3();
					obj = node[2].toN3();
					stmt = new StringBuilder(sub.length()+pre.length()+obj.length()+4);
					stmt.append(sub).append(" ").append(pre).append(" ").append(obj).append(" .");
					s_len = pre.length()+obj.length();
					
					l = node[2];
				// 1, allocate the triple to the right part	& put resource in parts index
					wmax=0f; 
					int tag1=0,tag2=0;
					Hashtable<Integer, Quad> temp, item;
					Quad detail = null;
					
					if(!(l instanceof Literal)){
						
					// o_len info is inside	
						o_len = pre.length()+sub.length();	
						if(sub.equals(obj)){ 
							ts = "s"; to = "o";            
						}else{	
							if(res.equals(sub)) {
								ts = "s";
								HashSet<Node[]> trip = str_buffer.get(obj);
								if(trip != null){
									trip.remove(node); 
									str_buffer.put(obj, trip);
								}
							}
							else {
								
								to = "o";
						/* (check index)
						 *   if the sub part is not in index, triples to this part; 
						 *   else if sub part in different part, assign this triple to the sub part
						 */	
								HashSet<Node[]> trip = str_buffer.get(obj);
								Quad slot;
								if(trip != null){
									trip.remove(node); 
									str_buffer.put(obj, trip);
								}
						//	find the largest weight sub part to place triple and res		
								Hashtable<Integer,Quad> plist = null;
								if(ind_buffer.containsKey(sub)){
									plist = ind_buffer.get(sub);
									wmax = 0f;
									for(int p : plist.keySet()){
										slot = plist.get(p);
									// for multiple sub, triple go to 
										if(slot.type.indexOf('s') != -1){
											if(p != assign.part){
												//find the max weight part to assign
												if(slot.weight > wmax) {
													wmax = slot.weight;
													sig1 = p;
												}
											}else {
										//		same part case
												sig1 = p;
												wmax = -1f;
												break;
											}
										}
									}			
								}
							// should consider the case that a res ind both in ind_buffer and mysql.
									
								if(index_flag != 0){
									parlist = mysql.searchIndex(sub);
									if(!parlist.isEmpty()){
										for(int p : parlist.keySet()){
											slot = parlist.get(p);
											if(slot.type.indexOf('s') != -1){
												if(p != assign.part){
													if(slot.weight > wmax){
														wmax = slot.weight; // if the weight is 0 for all, how about it?
														sig1 = p;
													}
												}else{
											// same part case		
													sig1 = p;
													wmax=-1f; // make it combination different from any cases
													break;
												}
											}
										}
									}
								}
							// not the same part as assign part, also have such slot in index for sub
								if(wmax != 0){ 
									spar = tp.get(sig1);
									spar.add(stmt.toString());
								// record the no of stmt info for each partition	
									parts_stmt_info[sig1]++;
								// decide obj go to the part that different from assign.part
									if(items != null){
										if(items.containsKey(sig1)){
											detail = items.get(sig1);
											if(detail.type.indexOf('o') == -1)
												detail.type = "so";
											detail.weight += 1-parts_stmt_info[sig1]/no_triples;
										}else{
											detail = new Quad();
											detail.type = "o";
											detail.weight = (float) (1-parts_stmt_info[sig1]/no_triples);
											parts_res_info[sig1]++; // update parts res no info
										}
									}else{
										items = new Hashtable<Integer, Quad>();
										detail = new Quad();
										detail.type = "o";
										detail.weight = (float) (1-parts_stmt_info[sig1]/no_triples); // use formula : 1-P(i)/C
										parts_res_info[sig1]++; // update parts res no info
									}		
							// put the res index into the largest sub weight part, put in bytes info
									detail.o_bytes += o_len;
									items.put(sig1, detail);
									ind_buffer.put(res, items);
								
							//update the sub index in sig1 part
								
									slot = plist.get(sig1);
									if(slot.type.indexOf('s') == -1){
										slot.type = "so";
									}
									slot.weight += 1-parts_stmt_info[sig1]/no_triples;
									slot.s_bytes += s_len; // byte info for sub res
									plist.put(sig1, slot);
									ind_buffer.put(sub, plist);
									
							// update str_nei by remove allocated one		
									HashSet<String> neis = str_nei.get(res);
									if(neis != null)
										neis.remove(sub);
									str_nei.put(obj, neis);
									neis = str_nei.get(sub);
									if(neis != null)
										neis.remove(obj);
									str_nei.put(sub, neis);
									flag = 1;
								}/*else{
								// it belongs to no partition, or same part, add it into this part
									
								}*/
								
							}
						}
					} else{
							ts = "s";
					}
					
				/*
				 * 3, before triple allocation, better to check if this res has already exist in index! if it is
				 * exist, check if the index entry has the same partition as this one. when checking, use 
				 * the type every combination. If it is different from the exsit one, especially the one with 
				 * type 's', then compare the weights, if the new one is bigger, put res in correct_list. Else
				 * check the weights of the two, and update it with formula (use database for index)
				 *  if new one smaller, use formula: n'w(t'-1,i), n' is the new added in res to the partition;
				 *  w(t'-1,i), the weight calculated before add in any new res--to update the current one.
				 */
			// 2, assign part case
					String ftype = ts+to;
					// decide the res type for assign part index
					if(finalty != null){
						if(!ftype.equals(finalty))
							finalty = "so";
					}else finalty = ftype;
			// if res is sub, or sub have not index, or they are in same part then place after assigned part		
					if(flag != 1){ 
						// allocate the triple
						par = tp.get(assign.part);
						par.add(stmt.toString());
					// update the record of stmt info in partition	
						parts_stmt_info[assign.part]++;
						HashSet<Node[]> trip = str_buffer.get(sub);
						if(trip != null){
							trip.remove(node); 
							str_buffer.put(sub, trip);
						}
					// if res is sub, put obj into the assigned part, else also put real sub in assigned part (another resource handle)
						if(res.equals(sub)){
						// statistic of sub bytes in one place inside loop, then add back outside	
							s_sum_len += s_len;
							
							if(!(l instanceof Literal)){
								if(ind_buffer.containsKey(obj)){
									temp = ind_buffer.get(obj);
									if(temp.containsKey(assign.part)){
										detail = temp.get(assign.part);
										if(!detail.type.equals("o"))
											detail.type = "so";
										detail.weight += 1-parts_stmt_info[assign.part]/no_triples;
									}else{
										newneis++;
										detail = new Quad();
										detail.type = "o";
										detail.weight = 1-parts_stmt_info[assign.part]/no_triples;
										parts_res_info[assign.part]++; //update res no info for partition 
									}
								}else{
									newneis++;
									temp = new Hashtable<Integer, Quad>();
									detail = new Quad();
									detail.type = "o";
									detail.weight = 1-parts_stmt_info[assign.part]/no_triples;
									parts_res_info[assign.part]++; //update res no info for partition 
								}
								detail.o_bytes += o_len; // bytes info
								temp.put(assign.part, detail);
								ind_buffer.put(obj, temp);
							}
						}else{
							o_sum_len += o_len;								
							
							if(sig1 != -1 && wmax == -1){ // also make it different from any case include initial case
								
								temp = ind_buffer.get(sub);
								if(temp != null){
									detail = temp.get(assign.part);
									if(detail != null){
										if(!detail.type.equals("s"))
											detail.type = "so";
										detail.type += 1-parts_stmt_info[assign.part]/no_triples;
									}
								}
							}else{ // no such index as sub
								newneis++;
								temp = new Hashtable<Integer, Quad>();
								detail = new Quad();
								detail.type = "s";
								detail.weight = 1-parts_stmt_info[assign.part]/no_triples;
								parts_res_info[assign.part]++; //update res no info for partition 
							}
							detail.s_bytes += s_len;
							temp.put(assign.part, detail);
							ind_buffer.put(sub, temp);	
						}
						HashSet<String> neis = str_nei.get(sub);
						if(neis != null){
							neis.remove(obj);
							str_nei.put(sub, neis);
						}
						neis = str_nei.get(obj);
						if(neis != null){
							neis.remove(obj);
							str_nei.put(sub, neis);
						}
					}
			// check spar size for writeout		
					if(spar != null && spar.size() > sizelimit)
						writeoutTri(assign.part,spar,parfolder);
				}// for
				
			// (final resource allocation after check all stmt related with one res) no matter what, the res still has allocation on assign part because of res as sub or they in same assign part
			// all neis have been allocated already, so only res need to add in assigned part;	
				if(finalty != null){
					Quad detail;
					if(items == null){
						items = new Hashtable<Integer, Quad>();
						detail = new Quad();
						detail.type = finalty;
						detail.weight = assign.type+newneis*(1-parts_stmt_info[assign.part]/no_triples);
					}else{
						detail = items.get(assign.part);
						if(detail != null){
							if(!detail.type.equals(finalty))
								detail.type = "so";
							detail.weight += assign.type+newneis*(1-parts_stmt_info[assign.part]/no_triples);
						} else{
							detail = new Quad();
							detail.type = finalty;
							detail.weight = assign.type+newneis*(1-parts_stmt_info[assign.part]/no_triples);
						}
					}
				//  decide the type of res for bytes statistic
				// need to cumulate previous o_bytes and s_bytes togather	
					detail.s_bytes += s_sum_len;
					
					detail.o_bytes += o_sum_len;
					items.put(assign.part, detail);
					ind_buffer.put(res, items);
					
				// update str_nei by remove allocated one		
					str_nei.remove(res);
				}
			
			// update parts_stmt_info	
			//  add triple of res into write_buffer of part_i 
				if(par != null && par.size() > sizelimit)
					writeoutTri(assign.part,par,parfolder);
				
			/*
			 *  writeout index buffer to mysql when is full.
			 *  also update the number of resource in the partition: parts_res_info.
			 */
				if(ind_buffer.size() >= INDEX_BUFFER_SIZE){
					mysql.createIndex(ind_buffer);
					ind_buffer.clear();
				}
			}
}
