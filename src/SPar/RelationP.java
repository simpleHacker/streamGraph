package SPar;

/**
 * main controlling code 
 * 1, read in triples from file set to cache by per-file
 * 2, 
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import SPar.allocation.AllocLoader;
import SPar.allocation.ResAlloc;
import SPar.assignment.ParAlgo;
import SPar.assignment.ParLoader;
import SPar.struct.Pair;
import SPar.struct.Quad;
import SPar.utility.DBOperation;
import SPar.utility.Statistic;

public class RelationP{
	int index_flag = 0; // initially to check if index is null 0/1
	int INDEX_BUFFER_SIZE = 1000;
	int STR_BUFFER_SIZE;  // incoming window size 100000
	int MAX = 500;
//	private static String DB;
//	private static String table;
	
	private static int parts; // needed partitioning part
	private static float no_triples; // total number of triples of each partition
	private static DBOperation mysql;
	String parfolder;
	String source;
	Hashtable<String, Hashtable<Integer,Quad>> ind_buffer; // <res, <partition,index_info> -- index buffer
	
	Hashtable<String, Hashtable<String,Integer>> str_nei; // add in neighbor frequency information
//	Hashtable<String, HashSet<String>> str_nei; // resource, neighbors -- neighbor list of streams
	Hashtable<String, HashSet<Node[]>> str_buffer; // res, triple nodes -- stream buffer
	int[] parts_res_info; // <partition, number of res>
	int[] parts_stmt_info; // <partiton, number of stmt>

	String symbol;
//  dynamic assign the resource and statement allocation class when call Dynamically
	public ResAlloc resAlloc;
	ParAlgo tp;
	
	public void setStrBuffer(int size){
		STR_BUFFER_SIZE = size;
	}
	
	public void setIndBuffer(int size){
		INDEX_BUFFER_SIZE = size;
	}
	
// for storing the triple partition	for buffered writing
//	ArrayList<ArrayList<String>> tp;
// symbol can use File.separator to replace, they are equal	
	public RelationP(String symbol, String filepath, int partNo, String parAlgo, String allocAlgo){
		Properties pro = new Properties();
		try{
	// read in all properties from file		
			pro.load(new FileInputStream(filepath+symbol+"config.properties"));
			int isize = Integer.parseInt(pro.getProperty("index_size"));
			int bsize = Integer.parseInt(pro.getProperty("stream_buffer_size"));
	//		int partNo = Integer.parseInt(pro.getProperty("part_number"));
			int tripleNo = Integer.parseInt(pro.getProperty("triple_number"));
			int wsize = Integer.parseInt(pro.getProperty("write_buffer_size"))/partNo;
			this.parfolder = pro.getProperty("partition_folder")+symbol+"GP_stmts_"+partNo;
			this.source = pro.getProperty("source_folder");
			String DB = pro.getProperty("db_name");
			String table = pro.getProperty("db_table");
			String port = pro.getProperty("db_port");
			String threshold = pro.getProperty("alloc_fill_percen"); // limit alloc algorithm put triple in partitions
	//		String parAlgo = pro.getProperty("random_strategy");
	//		String allocAlgo = pro.getProperty("alloc_strategy");
	// initiation
			ind_buffer = new Hashtable<String, Hashtable<Integer,Quad>>(isize);
			str_nei = new Hashtable<String, Hashtable<String,Integer>>(bsize);
			str_buffer = new Hashtable<String, HashSet<Node[]>>(bsize); // for saving the triples and index with type
			parts_res_info = new int[partNo];
			parts_stmt_info = new int[partNo];
			for(int i=0; i<parts;++i){
				parts_res_info[i] = 0;
				parts_stmt_info[i] = 0;
			}
			this.symbol = symbol;
			parts = partNo;
			no_triples = tripleNo/partNo;
			//mysql = new DBOperation(DB,table);
			mysql = new DBOperation(DB,table,port);
			this.resAlloc = AllocLoader.loadAlloc(allocAlgo,this.parfolder,symbol,parts,no_triples,mysql);
				//new ResAlloc(this.parfolder,symbol,parts,no_triples,mysql);
			this.resAlloc.setThreshold(1);
			this.tp = ParLoader.loadPar(parAlgo);
	// set buffer size		
			STR_BUFFER_SIZE = bsize;
			INDEX_BUFFER_SIZE = isize;
			this.resAlloc.setIndBuffer(isize);
			this.resAlloc.setWriteBuffer(wsize);
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}catch (Exception e){
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
/**
 * main function to read in triples, carry on allocation,
 * and place to a partition following strategy
 * 
 * cache in window size triples, construct neighborhood information so far in cache;
 * use resource allocation algorithm to decide a part for it;
 * update index for resource assignment follwing strategy pattern;
 * place triples in correspondent partition;
 * write out caeched-in index to outside mysql table when it fullanObject
 * 
 * @param filename
 * @throws IOException
 * @throws ParseException
 */
	private int counter = 0; // only used for record last number of stmt in buffer
	public void scan(String filename, int pos, int len) throws IOException, ParseException{
		InputStream is = new FileInputStream(filename);
		NxParser nxp = new NxParser(is);
		String sub, obj;
		Node[] ns;
		Node l;
		boolean a = false, b = false;
		int c;
		
 		while (nxp.hasNext()) {
 			// first, cache in enough triples into buffers		
			c= counter; // max = 0
		//	System.out.println("c initial size: "+c);
		//	String maxres; // degree max res consider first
			
			Hashtable<String,Integer> neis; //????
			HashSet<Node[]> tris;
			while (c<STR_BUFFER_SIZE){ // ( || a || b)
				
				if(nxp.hasNext()){
					ns = nxp.next();
					sub = ns[0].toN3();
					obj = ns[2].toN3();
					l = ns[2];	
				}else break;
				
				a=str_nei.containsKey(sub);
				
				if(!(l instanceof Literal)){
					b=str_nei.containsKey(obj);
				/** 
				 * add triples into str_buffer; attention, each triple is added twice
				 * due to triple gathered by two resources
				 */				
					if(!str_buffer.containsKey(sub))
						tris = new HashSet<Node[]>();	
					else tris = str_buffer.get(sub);
					tris.add(ns);
					str_buffer.put(sub, tris);
					if(!str_buffer.containsKey(obj))
						tris = new HashSet<Node[]>();
					else tris = str_buffer.get(obj);
					tris.add(ns);
					str_buffer.put(obj, tris);
					
				/**
				 * save res into str_nei as neighbor list, each res is added twice.
				 * due to inter-neighborhood relation
				 */ 		
					int nei_count=0;
					if(!obj.equals(sub)){
						if(!a) {
							neis = new Hashtable<String,Integer>();
							neis.put(obj, 1);
						}
						else {
							neis = str_nei.get(sub);
							if(neis.containsKey(obj))
								nei_count = neis.get(obj);
							neis.put(obj, nei_count+1);
						}
						str_nei.put(sub, neis);	
					
						nei_count=0;
					
						if(!b) {
							neis = new Hashtable<String,Integer>();
							neis.put(sub,1);
						}
						else {
							neis = str_nei.get(obj);
							if(neis.containsKey(sub))
								nei_count = neis.get(sub);
							neis.put(sub, nei_count+1);
						}
						str_nei.put(obj, neis);	
					}
				}else{
					// add triples into str_buffer as case of obj as string				
					if(!str_buffer.containsKey(sub))
						tris = new HashSet<Node[]>();	
					else tris = str_buffer.get(sub);
					tris.add(ns);
					str_buffer.put(sub, tris);
				}
				c++;
				
			}  // while
	/********************************test*******************************/		
	//		System.out.println("read out statements numbers: "+c);
	//also if c < buffer size, we shouldn't continue, because we need to cache in more statements from next file
			if(c<STR_BUFFER_SIZE && pos<len-1 ) {
				counter = c;
				break;
			}
			counter = 0; 
		/**
		 * check index && place triples, 
		 * sort str_nei list by each res's neighbor numbers"
		 */ 
		// do sort according to neighbor's number
			Hashtable<Integer,HashSet<String>> sortlist = new Hashtable<Integer, HashSet<String>>(str_nei.size());
			int no;
			HashSet<String> list;
	/****************************check********************************
			if(str_nei.size() < str_buffer.size()) // for this case, triple will be evenly distributed by balanceP
				System.out.println("some triples in str_buffer has no resource in str_nei");
	/********************************************************/		
			
			for(String res:str_buffer.keySet()){ // res:str_buffer.keySet();
				no = summup(str_nei.get(res));
				if(sortlist.containsKey(no))
					list = sortlist.get(no);
				else list = new HashSet<String>();
				list.add(res);
				sortlist.put(no, list);
			}
			
			Object[] tmp = null;
			tmp = sortlist.keySet().toArray();
			Arrays.sort(tmp);
		//  check sortlist and its neilist for res in desending order	
			Hashtable<Integer,Integer> sumlist;
		//	Hashtable<Integer,HashSet<String>> sumlist;
			for(int i=tmp.length-1;i>=0;--i){
				list = sortlist.get(tmp[i]);
				for(String res:list){
					if(str_buffer.get(res).isEmpty()) continue;
						
					neis = str_nei.get(res);
			/**
			 * get the join set information for assignment decision
			 * use res neiborlist join with res set of each partition;
			 * get the join set with each part, save to sumlist
			 * 
			 */  
					if(neis != null)
						sumlist = part_nei_join(neis);//
					else sumlist = null;
				/**
				 * assign a res to a part (interface)
				 * the assignment algorithm can be decide by send in a message. 
				 */ 	
/********************************test*******************************
					if(res.equals("<http://data.semanticweb.org/person/jasmin-opitz>") && sumlist != null){
						for(int t:sumlist.keySet())
							System.out.print(t+"+"+sumlist.get(t)+" ");
						System.out.println();
						for(String nn:neis.keySet())
							System.out.println(nn+STR_BUFFER_SIZE" "+neis.get(nn));
						checkInd(res);
					}
/********************************************************/				
	/*???
	 * if a res has already been assigned, if we check it in its new assignment with algorithm one
	 * not by balance, how to do? keep it was? or reassign?, so we also need to record the score for 
	 * the previous assigned partition  				
	 */				
				//	TrailP tp = new TrailP();
				//	balanced partition is default
					
					Pair<Integer,Integer> assign = tp.streamP(sumlist,parts_stmt_info, no_triples);
		/********************test*****************************
					if(res.equals("<http://data.semanticweb.org/person/jasmin-opitz>"))	
						System.out.print(" "+assign.part+"-"+assign.type);
					if(assign.type > 100){
						System.out.println(res+" assignment weight over large! is "+ assign.type+"; stmt_no: "+parts_stmt_info[assign.part]);
						for(int t:sumlist.keySet())
							System.out.print(t+"+"+sumlist.get(t)+" ");
						System.out.println();
					}
		/*****************************************************/		
		/**	
		 * count if buffer is full, if full, then write out the index to mysql for storage.
		 * update mysql index by createIndex if any index writeout event happend
		 * all global variable updates are here, str_buffer, part_stmt, res info
		 */
			//		if(res.indexOf(test) != -1)  System.out.println("wrongs here!");
					index_flag |= resAlloc.updateIndex(res,assign,str_buffer,str_nei,ind_buffer,index_flag,parts_stmt_info);
					
			//		System.out.println("index flag: "+index_flag);
					if(sumlist != null)
						sumlist.clear();
				}
			}
		/** clear str_buffer and str_nei after one streaming section processing finished */
		// only for ConResAlloc - obselated!
		//	resAlloc.subpart.clear();
		//	if(!str_buffer.isEmpty()) //!!!!!!!!!!!should check each item there
		//		System.err.println("Allocation algorithm did not clear the buffer!!!");
			
			str_buffer.clear();
			str_nei.clear();
		// status(); // test use	
	  } // outsize while
 		
		is.close();
		System.out.println("file "+filename+" end");
	}
	
// test method ind_buffer	
	public void checkInd(String nei){
		Hashtable<Integer, Quad> test = ind_buffer.get(nei);
		System.out.println("+++++++++++ind_buffer information for "+nei);
		for(int i: test.keySet()){
			System.out.println("part "+i+" Quad: "+test.get(i).toString());
		}
		System.out.println("---------------------------");		
	}
	
	public int summup(Hashtable<String,Integer> list){
		int sum = 0;
		if(list == null) return sum;
		for(String key:list.keySet()){
			sum += list.get(key);
		}
		return sum;
	}
	
	public boolean scanall(String folder){
		File dir = new File(folder);
		File[] files=dir.listFiles();
		ArrayList<String> par;
		try{
//		  memory usage report
			Runtime runtime = Runtime.getRuntime();
			System.out.println("*Stream partitioning Performance checker (below)");
			
			for(int i=0;i<files.length;++i){
				scan(folder+symbol+files[i].getName(),i,files.length);
			// check the memory usage every scan step	
				System.out.println("*Free memory: "+runtime.freeMemory());
				System.out.println("*Used memory: "+(runtime.totalMemory()-runtime.freeMemory()));
			}
			System.out.println("***"); // performance check end
		// write out the rest stmts in each part	
			for(int i=0;i<parts;++i){
				par = resAlloc.tp.get(i);
				if(par != null && !par.isEmpty()){
					resAlloc.writeoutTri(i,par,parfolder);
				}
			}
/*****************test code*********************
			Hashtable<Integer,Quad> testee = ind_buffer.get("<http://data.semanticweb.org/person/jasmin-opitz>");
			for(int tt: testee.keySet())
				System.out.print("-"+tt+" ");
			System.out.println();
/*******************test end***********************/			
		// write out all index of ind_buffer to database	
			if(!ind_buffer.isEmpty()){
				mysql.createIndex(ind_buffer);
	/**************************test checker************************/			
	//			this.resAlloc.updateInd(this.resAlloc.ind_checker, ind_buffer);
				ind_buffer.clear();
			}
			
	//		this.resAlloc.checkConsistency(this.resAlloc.ind_checker, this.resAlloc.sed_ind);
			return true;
		} catch(Exception e){
			e.printStackTrace();
			return false;
		}
	}
	
/**
 * collect infomation of res neighbor joining with each partition 
 * 
 * @param neis, neighbor list for a res node
 * @return collection by partitions
 */	
	public Hashtable<Integer,Integer> part_nei_join(Hashtable<String,Integer> neis){
	//  <part, neighbor list>	??should we put the frequency information for each nei in it. -- make sense, do it.
		Hashtable<Integer,Integer> sum = new Hashtable<Integer, Integer>();
	//	Hashtable<Integer,HashSet<String>> sumlist = new Hashtable<Integer, HashSet<String>>();
		Hashtable<Integer,Quad> tmp, tmp2;
		int total;	
	/**
	 * gather intersection between one's neighbors and each partition they appears
	 */ 	
		if(neis.isEmpty()) return null;
		Set<String> neilist = neis.keySet();
		HashSet<Integer> keylist = new HashSet<Integer>();
	// if neis is null, need to handle, sumlist related item is null	
		if(index_flag == 0){
			
			for(String nei : neilist){
				tmp = ind_buffer.get(nei);
		//		tmp = resAlloc.sed_ind.get(nei);
				if(tmp != null){
					for(int i : tmp.keySet()){
						if(sum.containsKey(i))
							total = sum.get(i);
						else total = 0;
						total += neis.get(nei);
						sum.put(i, total);
					}
				}
			}
		}else{
			for(String nei: neilist){
				
			// check the index from mysql and index buffer, and keep it in sumlist;	
				try{
					tmp = ind_buffer.get(nei);
					tmp2 = mysql.searchIndex(nei);
			//		tmp2 = resAlloc.ind_checker.get(nei);
					
					if(tmp != null)
						keylist.addAll(tmp.keySet());
					if(tmp2 != null)
						keylist.addAll(tmp2.keySet());
	
					for(int elem : keylist){
						if(sum.containsKey(elem)){
							total = sum.get(elem);
						}else total = 0;
						total += neis.get(nei);
						sum.put(elem, total);
					}
					keylist.clear(); // must clear after operation
				}catch(Exception e){
					System.out.println("wrong in MySql DB search!");
				}
			}
		}
		return sum;
	/**
	 * after sumlist is prepared, use it for algorithm work.
	 * if for some node, the sumlist is empty, assign it with balanced algorithm
	 */ 
	}
	
// check node dataset size status info	
	public void status(){
		int sum = 0;
	
		for(int i=0;i<this.parts_stmt_info.length;++i){
			System.out.print(parts_stmt_info[i]+" ");
			sum += parts_stmt_info[i];
		}
		System.out.println("total number: "+sum);
		System.out.println("statement buffer size: "+STR_BUFFER_SIZE);
	}
	
/**
 * calculate the workload balance of each partition
 * @param parts_stmt_info
 */	
	public static void nodebalance(int[] parts_stmt_info){
		double total=0;
		int size = parts_stmt_info.length;
		int sum = 0;
		for(int i = 0;i<size;++i){
			System.out.println(parts_stmt_info[i]);
			sum += parts_stmt_info[i];
		}
		double avg = sum / (double) size;
		
		 for(int i=0;i<size;++i){
			 int stmtsize = parts_stmt_info[i];
			 double temp = stmtsize - avg;
			 total += temp*temp;
		 }
		 
		 double dev = Math.sqrt(total/size);
		 
		 System.out.println("size balance deviation for partition "+size+" is: "+dev);
	}
	
/**
 * refresh the folder information	
 */
	
	public void refreshfolder(String folder){
		File file = new File(folder);
		if(file.exists()){
			File[] filelist = file.listFiles();
			for(int i=0;i<filelist.length;++i){
				String filepath = filelist[i].getPath();
				file = new File(filepath);
				file.delete();
			}
		}else{
			file.mkdir();
		}
	}
	
/**
 * test case with dataset "www-2012-complete.nt" into 20 partitions
 * with random equal partitioning
 * @param args
 */	
	
	public static void main(String[] args){
		
	/** get in parameters
	 * $0: parts number; $1: resource allocating strategy; $2: partition folder position; $3: dataset folder
	 * $4: statements buffer window size; $5: index buffer size; $6: total number of statement in dataset	
	 */
		
		/*String parfolder = "GP_stmts_20";
		String parPrefix = args[2];
		String parfolder = parPrefix+"GP_stmts_"+args[0];
		String symbol = "/";
		//int parts = 20;
		int parts = Integer.parseInt(args[0]);
		String source = args[3]; // data source folder
		//String source = "testset";
		String allocation = args[1];// res allocation strategy
		
	// size configuration (can also get from parameters)
		int strBuffer = Integer.parseInt(args[4]);//10000;
		int indBuffer = Integer.parseInt(args[5]);//1000000;
		float no_triple = Integer.parseInt(args[6]);//14898; can be larger than the real total triple number
		int wrtBufferTotal = Integer.parseInt(args[7]); // 20000;
		int wrtBuffer = wrtBufferTotal/parts;
		
		String port = "3306"; // port for mysql connection
		String db = "gindex";
		String dbtable = "sindex";
		float par_triple_no = no_triple/parts; // this can be given by capacity of the node
*/
		String symbol = File.separator;
		String filepath = args[0];
		int parNo = Integer.parseInt(args[1]);
		String parAlgo = args[2];
		String allocAlgo = args[3];
		RelationP test = new RelationP(symbol,filepath,parNo,parAlgo,allocAlgo);

	// preparation
		test.refreshfolder(test.parfolder);
				
		/** test case with trendy placement strategy
		 * test.resAlloc = new TrendyResAlloc(parfolder,symbol, parts, no_triple,test.mysql);
		 */
	//	test.resAlloc.setThreshold(0.5f);
		//test.resAlloc.setThreshold(percen);
		System.out.println("Index buffer size: "+test.INDEX_BUFFER_SIZE);
		
		Statistic sta = new Statistic();
		//test.nodebalance(test.parts_stmt_info);
		/** test case with sticky placement strategy
		 * test.resAlloc = new StickyResAlloc(parfolder, symbol, parts, no_triple, test.mysql);
		 */ 
		try{
			test.scanall(test.source); // core methods for stream partitioning
			System.out.println("parts stmt info: ");
			test.status();
			
			int sumcut = sta.sumCut(RelationP.mysql);
			System.out.println("total cut number is: "+sumcut);
			sta.nodebalance(test.parts_stmt_info);
	//  testbig temp setting, need to be remove for general purpose
	//		test.mysql.deleteAll();
			
			test.mysql.closeConnection();
			
		} catch(Exception e){
			e.printStackTrace();
		}
	}
	
}


