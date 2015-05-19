package trial;

import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.yars.nx.Node;

import SPar.struct.Pair;

public class TrailP {

// balanced version for trail	
	public Pair<Integer,Float> palgorithm(int[] parts_stmt_info, float no_triples){
		int min = parts_stmt_info[0];
		int index = -1;
		for(int i=1;i<parts_stmt_info.length;++i){
			if(parts_stmt_info[i] < min) {
				min = parts_stmt_info[i];
				index = i;
			}
		}
		Pair<Integer,Float> p = new Pair<Integer,Float>();
		if(index == -1){
			p.part = 0;
			p.type = 1f; 
		}else{
			p.part = index;
			p.type = 1-parts_stmt_info[index]/no_triples;
		}

		return p;
	}
}
