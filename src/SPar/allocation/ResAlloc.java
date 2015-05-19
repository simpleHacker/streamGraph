package SPar.allocation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.yars.nx.Node;

import SPar.struct.Pair;
import SPar.struct.Quad;
import SPar.utility.DBOperation;

public class ResAlloc {
//	buffer size for trigger pending write for partitions	
	protected static int sizelimit = 10000; 
	String parfolder;
	int INDEX_BUFFER_SIZE = 1000;
	static float threshold = 1;
	
	String symbol;
	int parts;
	float no_triples; // total number of triples from dataset
	DBOperation mysql;
// structure storing the triple partition for buffered writing
	public ArrayList<ArrayList<String>> tp;

/************************test checker*******************************	
	public Hashtable<String, Hashtable<Integer, Quad>> ind_checker;
	public Hashtable<String, Hashtable<Integer, Quad>> sed_ind;
/*******************************************************************/	
	
	public ResAlloc(String parfolder,String symbol,int parts, float tripleNo, DBOperation mysqlinst){
		this.parfolder = parfolder;
		this.symbol = symbol;
		this.parts = parts;
		this.no_triples = tripleNo;
		
	//	ind_checker = new Hashtable<String, Hashtable<Integer, Quad>>(3000);
	//	sed_ind = new Hashtable<String, Hashtable<Integer, Quad>>(3000);
		
		threshold = threshold * tripleNo;
		
		this.mysql = mysqlinst;
		tp = new ArrayList<ArrayList<String>>();
		for(int i=0;i<parts;++i){
			ArrayList<String> par = new ArrayList<String>();
			tp.add(par);
		}
	}
	
	public void setWriteBuffer(int capacity){
		sizelimit = capacity;
	}
	
	public void setIndBuffer(int size){
		this.INDEX_BUFFER_SIZE = size;
	}
	
// set threshold
	public void setThreshold(float percen){
		threshold = percen * no_triples;
	}
	
// interface to update index	
	public int updateIndex(String res, Pair<Integer, Integer> assign, Hashtable<String, HashSet<Node[]>> str_buffer,
			Hashtable<String, Hashtable<String,Integer>> str_nei,Hashtable<String, Hashtable<Integer, Quad>> ind_buffer,
			int index_flag, int[] parts_stmt_info){
		return 0;
	}
	
// write out statement to partition i
	public void writeoutTri(int part, ArrayList<String> stmts, String parfolder){		
			String filepath =parfolder+symbol+"partition_"+part;
			File file = new File(filepath);
			if(!file.exists()){
				try{
					file.createNewFile();
				}catch(IOException e){
					System.err.println("file create wrong!");
				}
			}
			try{
			//append write	
				BufferedWriter out = new BufferedWriter(new FileWriter(filepath,true));
				
				for(int i=0;i<stmts.size();++i){
					out.write(stmts.get(i)+"\n");
				} 
				out.close();
				stmts.clear();
			}catch(IOException e){
				e.printStackTrace();
				System.out.println(e.getMessage());
				System.err.println("wrong in wrtieout!");
			}
	}
	
// update Res information in ind_buffer in secure
	public void updateRes(String res, char type, int weight, int part,int bytes, Hashtable<String, Hashtable<Integer, Quad>> ind_buffer, int no){
		/***************************test******************************
		if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
			if(ind_buffer.containsKey(res)){
				if(ind_buffer.get(res).containsKey(5)){
					System.out.println("weight is :"+weight+";partNo: "+no);
					System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
				}		
			}
		//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
		//	System.out.println("weight is :"+weight+";partNo: "+no);
		/*************************************************************/
		
		Hashtable<Integer,Quad> temp = null;
		Quad detail = null;
		if(ind_buffer.containsKey(res)){
			temp = ind_buffer.get(res);
			if(temp.containsKey(part)){
				detail = temp.get(part);
				detail.weight += weight;
			}else{
				detail = new Quad();
				detail.weight = weight;
			}
		}else{
			detail = new Quad();
			detail.weight = weight;
		}
		if(type == 'o'){
			detail.type_o = 1;
			detail.o_bytes += bytes;
		}else{
			detail.type_s = 1;
			detail.s_bytes += bytes;
		}
		if(temp == null)
			temp = new Hashtable<Integer, Quad>();
		
		temp.put(part, detail); // must not be null for detail
		ind_buffer.put(res, temp);
		/***************************test******************************
		if(res.equals("<http://data.semanticweb.org/conference/iswc/2010/time/2010-11-11T15-30-00>"))
			if(ind_buffer.containsKey(res)){
			//	System.out.println("weight is :"+weight+";partNo: "+no);
				if(ind_buffer.get(res).containsKey(5))
					System.out.println("after record: "+ ind_buffer.get(res).get(5).toString());
			}
		//			System.out.println("pre record: "+ ind_buffer.get(res).get(5).toString());
		//	System.out.println("weight is :"+weight+";partNo: "+no);
		/*************************************************************/
	}
	
	/*** test methods ****	
	public void updateInd(Hashtable<String, Hashtable<Integer, Quad>> ind, Hashtable<String, Hashtable<Integer, Quad>> buffer){
		Hashtable<Integer,Quad> inter, table;
	******************test	***************************
		if(ind.containsKey("<http://data.semanticweb.org/conference/iswc/2010>"))
			if(ind.get("<http://data.semanticweb.org/conference/iswc/2010>").containsKey(11))
				System.out.println("entry "+ind.get("<http://data.semanticweb.org/conference/iswc/2010>").get(11).toString());
	*************************************************		
		Quad tmp;
		for(String key : buffer.keySet()){
			inter = buffer.get(key);
			if(!ind.containsKey(key)){
				table = new Hashtable<Integer,Quad>();
				for(int p: inter.keySet()){
					table.put(p, inter.get(p).clone());
				}
			}else{
				table = ind.get(key);
				for(int p : inter.keySet()){
					if(table.containsKey(p)) {
						
						tmp = table.get(p).update(inter.get(p));
						table.put(p, tmp);
					}
					else{
						table.put(p,inter.get(p).clone());
					}
				}
			}
			ind.put(key, table);
		}
		******************test	***************************
		if(ind.containsKey("<http://data.semanticweb.org/conference/iswc/2010>"))
			if(ind.get("<http://data.semanticweb.org/conference/iswc/2010>").containsKey(11))
				System.out.println("after entry "+ind.get("<http://data.semanticweb.org/conference/iswc/2010>").get(11).toString());
	*************************************************
	}
	
	public boolean checkConsistency(Hashtable<String, Hashtable<Integer, Quad>> ind, Hashtable<String, Hashtable<Integer, Quad>> ind2){//DBOperation mysql
		Hashtable<Integer, Quad> t1, t2 ;
		int sum = 0;
		for(String key : ind.keySet()){
		//	t2 = mysql.searchIndex(key);
			t2 = ind2.get(key);
			t1 = ind.get(key);
			if(t2.isEmpty()){
				System.err.println("key "+key+" is not in database");
				return false;
			}else{
				for(int p: t1.keySet()){
					if(!t2.containsKey(p)){
						System.err.println("key "+key+"; part "+p+" is not in database!");
						return false;
					}else if(!t1.get(p).equal(t2.get(p))){
						System.err.println("key "+key+"; part "+p+" not has the same entry!");
						System.out.println("1st elem: "+t1.get(p).toString());
						System.out.println("2st elem: "+t2.get(p).toString());
						sum++;
					}
				}
			}
		}
		System.err.println("total "+sum+" different entries!");
		if(sum > 0) System.exit(1);
		return true;
	}
	
	public void showInd(Hashtable<String, Hashtable<Integer, Quad>> ind){
		Hashtable<Integer, Quad> elem;
		for(String key : ind.keySet()){
			elem = ind.get(key);
			for(int p : elem.keySet())
				System.out.println("("+key+","+p+","+elem.get(p).toString()+")");
		}
	}
	/***************************************************/

}
