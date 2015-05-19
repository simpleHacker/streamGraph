package SPar.assignment;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import org.semanticweb.yars.nx.Node;

import SPar.struct.Pair;

/**
 * LRU based balancing strategy, also keep locality info for random balancing assignment
 * For this strategy, must give a threshold to start a new part without fill former one full.
 * we give a relax factor to control the fill by balance and create new part for allocation,
 * in this way, the triple can spread out, and make streamP more workable, and can keep partial
 * locality information for streams
 * 
 * best one so far
 * 
 * e.g. relax factor: 0.3, means locality play half role as streamP
 */	

public class LRU extends ParAlgo{

	public LRU(){
		super();
		relax_f = 0.30f;
	}
	
/** 
 * LRU random assignment algorithm
 */	
	public void Adjust(int newpart, int[] parts_stmt_info, float no_triples){
//		 remove the full entry of triples from cache, not consider after for balanced plan
		Set<Integer> set = new HashSet<Integer>();
		set.addAll(ages.keySet());
		for(int key:set){
			if(parts_stmt_info[key] >= no_triples){
				ages.remove(key);
			}	
		}
		
//		update all ages info, also find the oldest key
		int age;
		int max = 0;
		int index = 0;
		ages.put(newpart, 0);
		for(int key:ages.keySet()){
			age = ages.get(key);
			ages.put(key, age+1);
			if(age+1 > max){
				index = key;
				max = age+1;
			}
		}
		
		if(ages.size() > slots)
			ages.remove(index);
		pre_part = newpart;
	}
	
	
/** 
 * factored block based balance random
 */ 
 	public Pair<Integer,Integer> balanceP(int[] parts_stmt_info, float no_triples){
		Pair<Integer,Integer> select = new Pair<Integer,Integer>();
		
		if(parts_stmt_info[pre_part] < no_triples*relax_f){ 
			select.part = pre_part;
			select.type = 0;
			
		}else{
			int min = parts_stmt_info[0];
			int index = -1;
			
			for(int i=1;i<parts_stmt_info.length;++i){
				if(parts_stmt_info[i] < min) {
					min = parts_stmt_info[i];
					index = i;
				}
			}
				
			select.part = index;
			select.type = 0;
		}
		Adjust(select.part,parts_stmt_info,no_triples);
		return select;
	}
	
	
/**
 * Locality reserved balanced partitioning
 * block partitioning feature adopts
 * pre_part here never related to streamP
 * 
 * less good than LRU balancing strategy
 * less good than total random strategy
 *
	
	public Pair<Integer,Integer> balanceP(int[] parts_stmt_info, float no_triples){
		Pair<Integer,Integer> p = new Pair<Integer,Integer>();
		int min = parts_stmt_info[0];
		int index = -1;
		if(parts_stmt_info[pre_part] < no_triples*relax_f){
			p.part = pre_part;
			p.type = 0;
		}else{
			if(pre_part+1 < parts_stmt_info.length){
				if(parts_stmt_info[pre_part+1] == 0){
					p.part=pre_part+1;
					p.type = 0;
					index = p.part; // index != -1	
				}
			} // next new partition 
			if(index == -1){ // former partition -- would not be used!!! Yes
				for(int i=1;i<parts_stmt_info.length;++i){
					if(parts_stmt_info[i] < min) {
						min = parts_stmt_info[i];
						index = i;
					}
				}
				if(index == -1) index = 0;
				p.part = index;
				p.type = 0;
			}
		}
		pre_part = p.part;
		return p;
	}*/
	
}
