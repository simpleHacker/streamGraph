package SPar.allocation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;

import SPar.struct.Pair;
import SPar.struct.Quad;
import SPar.utility.DBOperation;

/**
 * 
 * @author AI
 *
 */
public class SmartResAlloc extends ResAlloc {
	public SmartResAlloc(String parfolder, String symbol, int parts, float tripleNo, DBOperation mysql) {
		super(parfolder, symbol, parts, tripleNo, mysql);
	}

	public int updateIndex(String res, Pair<Integer, Integer> assign, Hashtable<String, HashSet<Node[]>> str_buffer,
			Hashtable<String, Hashtable<String,Integer>> str_nei,Hashtable<String, Hashtable<Integer, Quad>> ind_buffer,
			int index_flag, int[] parts_stmt_info){
				HashSet<Node[]> trilist;  
				Node l;
				String sub, obj, pre;
				int ts, to,fts=0, fto=0;
				StringBuilder stmt;
				ArrayList<String> par, spar;
				int s_len, o_len;
//				String partfile = "partition_"+assign.part;
//				if(exist_partition.contains(partfile)) changed_partition.add(partfile);
		/*
		 * 1, After assign all triples to one partition, need to remove those allocated triples from str_buff
		 * 2*, if want overlap, no need to remove 
		 */
				trilist = str_buffer.get(res);
				int flag; // insert judge (0: this part; 1:other part)
				Hashtable<Integer, Quad> items=null, parlist = null, merges;
		/**************************test checker***************************
					if(sed_ind.containsKey("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>")){
						if(sed_ind.get("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>").containsKey(5))
							System.out.println("smart pre record: "+ sed_ind.get("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>").get(5).toString());
					}
		/*************************************************************/		
				
				if(index_flag != 0)
					parlist = mysql.searchIndex(res);
					
				if(ind_buffer.containsKey(res))
					items = ind_buffer.get(res);
				
	 // merge two lists togather for weight and part consideration at the same place	
				merges = mergelist(items,parlist);
				
				int newneis = 0; // No. of new neis added in the assigned part index
				Float wmax = 0f;
				Float reswei = assign.type*(1-parts_stmt_info[assign.part]/no_triples); // used to compared with another vertex weight
				int sig = -1;
				float twei;
	//  check the history for current resource allocation, keep original or adjust to a new partition after par finished		
				if(merges != null){
					if(!merges.containsKey(assign.part)){ //update index in that part
					// check for adjustment !!!!!!!
						for(int i: merges.keySet()){
							Quad tmp = merges.get(i);
							twei = tmp.weight*(1-parts_stmt_info[i]/no_triples);
							if(twei > wmax){
								wmax = twei;
								sig = i;
							}
						}
						if(sig != -1){
					/** check balance and calculate the weight!!!!!!!!!!!!!!!!!!	*/	
							twei = reswei;
							if(wmax > twei && parts_stmt_info[sig] < threshold){ // constrained also by part size
							// keep original
								assign.part = sig; // assign res and triple to the largest
								reswei = wmax; 
							}/*else{
							/** Not to do adjust !!!!!	
							 * because adjustment cost is so high,
							 * so here, even need adj, don't do it!
							 
							  adj_list.add(assign.part, assign.weight, all ajust part list);
							}*/
						}
					}
				}
				
				Set<Node[]> tempset = new HashSet<Node[]>();
				tempset.addAll(trilist);
				
				int s_sum = 0, o_sum = 0;
				
				for(Node[] node : tempset){	
					spar = null;
					par = null;
				// decide the real assign part by checking all index options before everything
					int sig1 = -1;
	
					flag = 0;
					s_len = 0; o_len = 0;
					ts=0; to=0;
				// check each one, then remove each one from the others.	
					
					sub = node[0].toN3();
					pre = node[1].toN3();
					obj = node[2].toN3();
					stmt = new StringBuilder(sub.length()+pre.length()+obj.length()+4);
					stmt.append(sub).append(" ").append(pre).append(" ").append(obj).append(" .");
					l = node[2];
					s_len = pre.length()+obj.length();
				// 1, allocate the triple to the right part	& put resource in parts index
					wmax=0f; 
					Hashtable<Integer, Quad> temp;
					Quad detail;
					Hashtable<Integer,Quad> plist = null;
					
					if(!(l instanceof Literal)){
						o_len = pre.length()+sub.length();
						
						if(sub.equals(obj)){ 
							ts = 1; to = 1;   
							s_sum += s_len;
						}else{	
							if(res.equals(sub)) {
								ts = 1;
								s_sum += s_len;
								HashSet<Node[]> trip = str_buffer.get(obj);
								if(trip != null){
									trip.remove(node); 
									str_buffer.put(obj, trip);
								}
							}
							else {
								o_sum += o_len;
								to = 1;
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
						/**	
						 * find the largest weight sub part to place triple and res, 	
						 * and if in history partition, sub res exist in the same part as the res assignment,
						 * in this case, directly assign this sub and triple to that partition
						 */	
								plist = null;
								parlist = null;
								
								if(index_flag != 0)
									plist = mysql.searchIndex(sub);
									
								if(ind_buffer.containsKey(sub))
									parlist = ind_buffer.get(sub);
								
							// merge two lists togather for weight and part consideration at the same place	
								plist = mergelist(plist,parlist);
		
								if(plist != null){	
									wmax = 0f;
									for(int p : plist.keySet()){
										slot = plist.get(p);
									// for multiple sub, triple go to 
										if(slot.type_s == 1){
											if(p != assign.part){
												//find the max weight part to assign
												twei = slot.weight*(1-parts_stmt_info[p]/no_triples);
												if((twei > wmax) && (parts_stmt_info[p] < threshold)) {
													wmax = twei;
													sig1 = p;
												}
											}else{
										//		same part case, keep locality information
												sig1 = p;
												wmax = -1f;
												break;
											}
										}
									}			
								}
							
							// not the same part as assign part, also have such slot in index for sub
							// !!!!!!!!!should change to compare the weight with obj, who larger, follow who	
								if(wmax > 0){ // if wmax == 0, sig1 will be -1, so here only need one judgement
									spar = tp.get(sig1);
									spar.add(stmt.toString());
									
									if(trip != null){
										trip.remove(node); 
										str_buffer.put(sub, trip);
									}
									
									parts_stmt_info[sig1]++;
									
								// decide obj go to the part that different from assign.part
									int upweight = 1;
									// put the res index into the largest sub weight part
									updateRes(res, 'o', upweight, sig1, o_len, ind_buffer, parts_stmt_info[sig1]);
								//	updateRes(res, 'o', upweight, sig1, o_len, sed_ind, parts_stmt_info[sig1]);
								
							//also need to update the sub index in sig1 part!!!!!!!!!!!???????????
									updateRes(sub, 's',upweight, sig1, s_len, ind_buffer, parts_stmt_info[sig1]);
								//	updateRes(sub, 's',upweight, sig1, s_len, sed_ind, parts_stmt_info[sig1]);									
									
									flag = 1;
								}/*else{
								// it belongs to no partition, or same part, add it into this part
									
								}*/
							}
						}
					} else{
						ts = 1;
					}
					
				/*
				 * 3, before triple allocation, better to check if this res has already exist in index! if it is
				 * exist, check if the index entry has the same partition as this one. when checking, use 
				 * the type checking too. If it is different from the exsit one, especially the one with 
				 * type 's', then compare the weights, if the new one is bigger, put res in correct_list. Else
				 * check the weights of the two, and update it with formula (use database for index)
				 *  if new one smaller, use formula: n'w(t'-1,i), n' is the new added in res to the partition;
				 *  w(t'-1,i), the weight calculated before add in any new res--to update the current one.
				 */
			// 2, assign part case
					// decide the res type for assign part index
					fts |= ts;
					fto |= to;
			// if res is sub, or sub have no index, or they are in same assign part then place after assigned part		
					if(flag != 1){ // res assigned in assignment part
						// allocate the triple
						par = tp.get(assign.part);
						par.add(stmt.toString());
						
					//	if(parts_stmt_info[assign.part] > no_triples)
					//		System.err.println("total number of triples: "+no_triples+" : "+parts_stmt_info[assign.part]+" : "+assign.type);
							
						parts_stmt_info[assign.part]++;
						
						HashSet<Node[]> trip = str_buffer.get(sub);
						if(trip != null){
							trip.remove(node); 
							str_buffer.put(sub, trip);
						}
					// if res is sub, put obj into the assigned part, else also put real sub in assigned part (another resource handle)
						int upweight = 1;
						
						newneis++; // for all
						if(!sub.equals(obj)){
							if(res.equals(sub)){
								if(!(l instanceof Literal)){
									updateRes(obj, 'o', upweight, assign.part, o_len, ind_buffer, parts_stmt_info[assign.part]);
							//		updateRes(obj, 'o', upweight, assign.part, o_len, sed_ind, parts_stmt_info[assign.part]);
								}
							
							}else{ // res as obj
						// if sub is as res the same part as assignment, also need to handle
								updateRes(sub, 's', upweight, assign.part, s_len, ind_buffer, parts_stmt_info[assign.part]);
							//	updateRes(sub, 's', upweight, assign.part, s_len, sed_ind, parts_stmt_info[assign.part]);
							}
						}
							
						/*if(!sub.equals(obj)){	
							updateNeis(str_nei, sub, obj);
							updateNeis(str_nei, obj, sub);// when obj is Literal, still work
						}*/
					}
			// check spar size for writeout		
					if(spar != null)
						if(spar.size() > sizelimit)
							writeoutTri(sig1,spar,parfolder);
		    // add triple of res into write_buffer of part_i 
					if(par != null)
						if(par.size() > sizelimit)
							writeoutTri(assign.part,par,parfolder);
				}// for
			// no matter what, the res still has allocation on assign part because of res as sub or they in same assign part
			// all neis have been allocated already, so only res need to add in assigned part;	
			// if assign.type + newneis != 0, can allocate, or it already assigned to other parts, so should no assignment here
				Quad detail = null;
				int tjoin = newneis;
				Hashtable<Integer, Quad> ttemp=null;
				if((fts|fto) != 0 && tjoin > 0){ 
					/***************************test******************************
					if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
						if(ind_buffer.containsKey(res)){
							if(ind_buffer.get(res).containsKey(5)){
								System.out.println("weight is : "+tjoin+";bytes: "+s_sum+":"+o_sum);
								System.out.println("--pre record: "+ ind_buffer.get(res).get(5).toString());
							}		
						}
					//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
					//	System.out.println("weight is :"+weight+";partNo: "+no);
					/*************************************************************/
					if(ind_buffer.containsKey(res))
						ttemp = ind_buffer.get(res);
					else
						ttemp = new Hashtable<Integer, Quad>();
						
					if(ttemp.containsKey(assign.part))
						detail = ttemp.get(assign.part);
						
					if(detail != null){
						detail.weight += tjoin;
					} else{
						detail = new Quad();
						detail.weight = tjoin;
					}
					
					detail.type_s |= fts;
					detail.type_o |= fto;
			// if already in the assign part, no need to re-check the weight, 
			//	because the new weight will cover the old weight		
					detail.s_bytes += s_sum;
					detail.o_bytes += o_sum;

					ttemp.put(assign.part, detail);
					ind_buffer.put(res, ttemp);
					
					/***************************test******************************
					if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
						if(ind_buffer.containsKey(res)){
						//	System.out.println("weight is :"+weight+";partNo: "+no);
							if(ind_buffer.get(res).containsKey(5))
								System.out.println("--after record: "+ ind_buffer.get(res).get(5).toString());
						}
					//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
					//	System.out.println("weight is :"+weight+";partNo: "+no);
					/*************************************************************/
					
			/***********************test checker*********************
					
					if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
						if(sed_ind.containsKey(res)){
							if(sed_ind.get(res).containsKey(5)){
								System.out.println("weight is : "+tjoin+";bytes: "+s_sum+":"+o_sum);
								System.out.println("--pre record: "+ sed_ind.get(res).get(5).toString());
							}		
						}
					//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
					//	System.out.println("weight is :"+weight+";partNo: "+no);
					
						
					ttemp = null;
					detail = null;
					if(sed_ind.containsKey(res))
						ttemp = sed_ind.get(res);
					else
						ttemp = new Hashtable<Integer, Quad>();
						
					if(ttemp.containsKey(assign.part))
						detail = ttemp.get(assign.part);
						
					if(detail != null){
						detail.weight += tjoin;
					} else{
						detail = new Quad();
						detail.weight = tjoin;
					}
					
					detail.type_s |= fts;
					detail.type_o |= fto;
			// if already in the assign part, no need to re-check the weight, 
			//	because the new weight will cover the old weight		
					detail.s_bytes += s_sum;
					detail.o_bytes += o_sum;

					ttemp.put(assign.part, detail);
					sed_ind.put(res, ttemp);
					
					
					if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
						if(sed_ind.containsKey(res)){
						//	System.out.println("weight is :"+weight+";partNo: "+no);
							if(sed_ind.get(res).containsKey(5))
								System.out.println("--after record: "+ sed_ind.get(res).get(5).toString());
						}
					//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
					//	System.out.println("weight is :"+weight+";partNo: "+no);
					/*************************************************************/
					
			/**********************************************************/		
					
				// update str_nei by remove allocated one -- operation safe for hashset		
				//	str_nei.remove(res); no need, already done in updateNeis()
				}
				
			/*
			 *  writeout index buffer to mysql when is full.
			 *  also update the number of resource in the partition: parts_res_info.
			 */
				if(ind_buffer.size() >= INDEX_BUFFER_SIZE){
//					writeoutInd(lucene.FILES_TO_INDEX_DIRECTORY);
					mysql.createIndex(ind_buffer);
					/*******************test checker*********************
					//			showInd(ind_buffer);
								updateInd(ind_checker, ind_buffer);
								checkConsistency(ind_checker,sed_ind);
					//			System.out.println("sed_ind current size: "+sed_ind.size());
					/****************************************************/
					
					ind_buffer.clear();
					return 1;
//					changed_partition.clear();
				}
				return 0;
			// update exist files after index updates	
			//	exist_partition.add(partfile);
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
	
	public Hashtable<Integer, Quad> mergelist(Hashtable<Integer, Quad> buffer, Hashtable<Integer, Quad> db){
		if(buffer == null) return db;
		if(db == null) return buffer;
		Set<Integer> l1 = buffer.keySet(); // 
		Set<Integer> l2 = db.keySet();
		HashSet<Integer> l = new HashSet<Integer>();
		l.addAll(l1); l.addAll(l2);
		Quad p1,p2;
	
		Hashtable<Integer, Quad> merge = new Hashtable<Integer, Quad>();
		for(int i: l){
			Quad p = new Quad();
			p1 = buffer.get(i);
			p2 = db.get(i);
			if(p1 != null){
				p.type_s = p1.type_s;
				p.type_o = p1.type_o;
				p.weight = p1.weight;
				p.o_bytes += p1.o_bytes;
				p.s_bytes += p1.s_bytes;
			}

			if(p2 != null){	
				p.type_s |= p2.type_s;
				p.type_o |= p2.type_o;
				p.weight += p2.weight;
				p.o_bytes += p2.o_bytes;
				p.s_bytes += p2.s_bytes;
			}
			merge.put(i, p);
		}
		return merge;
	}

	
}
