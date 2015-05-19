package SPar.assignment;

import java.util.HashSet;
import java.util.Set;

import SPar.struct.Pair;

/**
 * cache strategy to find the next balanced locality assigning part
 * entry get out follows LRU according to ages info of each entry
 * pre_part locality selection follows most frequent used recently
 *  
 */ 
public class LRUF extends ParAlgo{
	
	int zone = 50; // LRUF only consider the recent N time visit for frequency
	
	public LRUF(){
		super();
		relax_f = 0.30f;
	}
	/**
	 * LRUF random assignment algorithm
	 */
		public void Adjust(int newpart, int[] parts_stmt_info, float no_triples){
			int count;
		// remove the full entry of triples from cache, not consider after for balanced plan
			Set<Integer> set = new HashSet<Integer>();
			set.addAll(cache.keySet());
			for(int key:set){
				if(parts_stmt_info[key] >= no_triples){
					cache.remove(key);
					ages.remove(key);
				}	
			}
				
//	 update history cache, assign next locality balancing part to the current most frequent part
			
			if(cache.containsKey(newpart)){
				count = cache.get(newpart);
				if(count <= zone) // limit frequency info within zone range
					cache.put(newpart, count+1);
			}else{
				cache.put(newpart, 1);
			}
			
//		update all ages info, also find the oldest key
			int age;
			int max = 0;
			int maxC = -1;
			int ageT = 0;
			int index = 0;
			ages.put(newpart, 0);
			for(int key:ages.keySet()){
				age = ages.get(key);
				ages.put(key, age+1);
				if(age+1 > max){
					index = key;
					max = age+1;
				}
				count = cache.get(key);
		// should decrease the oldest one by 1, if the picked one is the newpart, no deduct 		
				if(count > maxC){
					pre_part = key;
					maxC = count;
					ageT = age;
				}else if(count == maxC && age < ageT){ // when equal frequent, choose younger
					pre_part = key;
					ageT = age;
				}
			}
			count = cache.get(index);
			if(count > 0 && index != newpart) // reduce the oldest one's count
				cache.put(index, count-1);
			
//	 when slot is full, select the oldest to remove!		
			if(cache.size() > slots){ // all slots are for prepart
				cache.remove(index);
				ages.remove(index);
				
				if(index == pre_part){ // if pre_part is removed, find second largest clustering part
					max = 0;
					maxC = -1;
					for(int key:cache.keySet()){
						age = ages.get(key);
						count = cache.get(key);
				// should decrease the oldest one by 1, if the picked one is the newpart, no deduct 		
						if(count > maxC){
							pre_part = key;
							maxC = count;
							ageT = age;
						}else if(count == maxC && age < ageT){ // when equal frequent, choose younger
							pre_part = key;
							ageT = age;
						}
					}
				}
			}
		}
		
		int parts_stmt_filled_flag= 0;
		
		public Pair<Integer,Integer> balanceP(int[] parts_stmt_info, float no_triples){
			Pair<Integer,Integer> select = new Pair<Integer,Integer>();
			
			if(parts_stmt_info[pre_part] < no_triples*relax_f){ 
				select.part = pre_part;
				select.type = 0;
			}else{
				int min = parts_stmt_info[0];
				int index = -1;
				if(parts_stmt_filled_flag != 0 && parts_stmt_info[pre_part] < no_triples) // also can use LRU
					index = pre_part;
				else{
					for(int i=1;i<parts_stmt_info.length;++i){
						if(parts_stmt_info[i] < min) {
							min = parts_stmt_info[i];
							index = i;
						}
					}
					if(parts_stmt_filled_flag ==0 && min > 0) 
						parts_stmt_filled_flag = 1;
				}
				select.part = index;
				select.type = 0;
			}
			Adjust(select.part,parts_stmt_info,no_triples);
			return select;
		}
}
