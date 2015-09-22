package SPar;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import SPar.allocation.DHT;
import SPar.struct.Part;
import SPar.struct.Quad;
import SPar.utility.DBOperation;
import SPar.utility.HashFunction;
import SPar.utility.Statistic;

public class RandomP {
	int index_flag = 0; // initially to check if index is null 0/1
	int INDEX_BUFFER_SIZE =  100000;
	int STR_BUFFER_SIZE = 100000;
//	int MAX = 500;
//	private static String table = "gindex";
	
	private static int parts; // needed partitioning part
//	private static int no_triples; // total number of triples from dataset
	private static DBOperation mysql;
	
//	Hashtable<String, ArrayList<Pair<Integer,String>>> ind_buffer; // <res, <partition,type:weight>[]> -- index buffer
	Hashtable<String, Hashtable<Integer,Quad>> ind_buffer;
	Hashtable<String, HashSet<Node[]>> str_buffer; // res, triple nodes -- stream buffer
	int[] parts_res_info; // <partition, number of res>
	int[] parts_stmt_info; // <partiton, number of stmt>

	String symbol;

// replace for different partitioning algorithm	
	DHT dht;
	String stmtParFolder = "DHT_stmts";
	
	public RandomP(int partNo, String parfolder, String symbol, int tripleNo, int isize, int ssize, String db, String dbtable){
		INDEX_BUFFER_SIZE = isize;
		STR_BUFFER_SIZE = ssize;
		
		ind_buffer = new Hashtable<String, Hashtable<Integer,Quad>>(isize);
		str_buffer = new Hashtable<String, HashSet<Node[]>>(ssize); // for saving the triples and index with type
		parts_res_info = new int[partNo];
		parts_stmt_info = new int[partNo];
		stmtParFolder = parfolder;
		for(int i=0; i<partNo;++i){
			parts_res_info[i] = 0;
			parts_stmt_info[i] = 0;
		}
		this.symbol = symbol;
		parts = partNo;
//		no_triples = tripleNo;
		mysql = new DBOperation(db,dbtable);
	// need to instantiate the algorithm class -- change the code for different class
		HashFunction SHA1 = new HashFunction();
		int numberRep = 1;
		dht = new DHT(SHA1,numberRep,parts,parfolder,this.symbol);
		
	}
	
//  use gradual read model to read in and store all statements	 
	 public boolean store(String filename) throws IOException{
		    InputStream is = new FileInputStream(filename);
			NxParser nxp = new NxParser(is,true);	
			String[] stmt = new String[3];
			while (nxp.hasNext()) {
				Node[] ns = nxp.next();
				Node l = ns[2];
				
				String sub = ns[0].toN3();
				String pro = ns[1].toN3();
				String obj = ns[2].toN3();					

				if(!(l instanceof Literal)){	
		// convert stmt format for processing ,can improved!			
					stmt[0] = sub;
					stmt[1] = pro;
					stmt[2] = obj;
				}else{
					stmt[0] = "?"+sub;
					stmt[1] = pro;
					stmt[2] = obj;
				}
				
	/*INDEX_BUFFER_SIZE
	 * Replacement part for different random res and triple allocation algorithm
	 * 			
	 */
			// assign the res to a partition
				Part node = dht.dht_assign(sub);
			// update index and put triple into partition	
				if(node != null)
					if(!dht.updateIndex(sub,node,stmt,ind_buffer,parts_stmt_info)){
						System.err.println("insert value has wrong!");
						return false;
					}
			}
			is.close();
			return true;
	 }
	 
	public void status(){
		for(int i=0;i<this.parts_stmt_info.length;++i)
			System.out.print(parts_stmt_info[i]+" ");
		System.out.println();
	}

// need to write out ind_buffer anyway	
	public boolean storeall(String folder){
		File dir=new File(folder);
		File[] files=dir.listFiles();
		try{
			for(int i=0;i<files.length;++i){
				store(folder+symbol+files[i].getName());
			//@test	
				System.out.println(files[i].getName());
			}
		// write out all index rest in ind_buffer	
			mysql.createIndex_random(ind_buffer);
			ind_buffer.clear();
		// write out the rest stmts in each part	
			Collection<Part> parts = dht.circle.values(); 
			for(Part node : parts){
				writeout(node.stmts,node.stmtfile,this.stmtParFolder,this.symbol);
		//		String p = node.stmtfile.substring(node.stmtfile.indexOf("_")+1);
		//		writeout_res(node.resource_list,"resource_of_part_"+p);
				node.stmts.clear(); 
			}
			return true;
		} catch(Exception e){
			e.printStackTrace();
			return false;
		}   
	}
	
	// write out the satements into a certain file
	public static boolean writeout(Iterable stmts, String filename, String stmtParFolder, String symbol){
		String filepath = stmtParFolder+symbol+filename;
		File file = new File(filepath);
		if(!file.exists()){
			try{
				file.createNewFile();
			}catch(IOException e){
				System.err.println("file create wrong!");
			}
		}
		try{			
			BufferedWriter out = new BufferedWriter(new FileWriter(filepath,true));
			
			String value;
			for(Object elem : stmts){
				value = (String) elem;
				out.write(value+"\n");
			}
			out.close();
			return true;
		}catch(IOException e){
			System.out.println(e.getMessage());
			return false;
		}
	 }
	 
	 public static boolean writeout_res(HashSet<String> res, String filename, String stmtParFolder, String symbol){		 
		 String filepath = stmtParFolder+symbol+filename;
//			 System.out.println(filepath);
		 File file = new File(filepath);
		 if(!file.exists()){
			try{
				file.createNewFile();
			}catch(IOException e){
				System.err.println("file create wrong!");
			}
		 }
			 			
		try{ 
			BufferedWriter out = new BufferedWriter(new FileWriter(filepath));
		
			String type;
			String re;
			int index;
			for(String elem : res){
				try{
					index = elem.indexOf("|");
					if(index == -1 || index > 2) System.err.println("error in DHT!");
					//	index = elem.indexOf("_:");
					type = elem.substring(0, index);
					re = elem.substring(index+1);
			// test
			//		System.out.println(re+" "+type);
					out.write(re+" "+type+"\n");
				}catch(Exception e){
					System.out.println(e.getMessage());
					System.out.println("wierd String: "+elem);
					return false;
				}
			}
			out.close();
		}catch(IOException e){
			System.out.println(e.getMessage());
			return false;
		}
			return true;
					 
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
	 
	 public static void main(String[] args){
		 /** get in parameters
			 * $0: parts number; $1:  dataset folder; $2: partition folder position; 
			 * $3: statements buffer window size; $4: index buffer size; $5: total number of statement in dataset	
			 */ 
		 
			//String parfolder = "DHT_stmts_20";
		 	String partitionPrefix = args[2]; //partition store position
			String parfolder = partitionPrefix+"DHT_stmts_"+args[0];
			String symbol = "/";
			int parts = Integer.parseInt(args[0]);//20;	
			String source = args[1];
			//String source = "testset";
			
		// configuration	
			int indBufferSize = Integer.parseInt(args[4]);//1000000;
			int strBufferSize = Integer.parseInt(args[3]);//1000000;
			int no_triple = Integer.parseInt(args[5]);//14898;
			int wrtBufferTotal = Integer.parseInt(args[6]);
			int wrtBuffer = wrtBufferTotal/parts;
			
		//	int capacity = 1000000; // pending writeout stmts buffer size
			String db = "gindex";
			String dbtable = "sindex";
			
			RandomP test = new RandomP(parts, parfolder, symbol, no_triple, indBufferSize, strBufferSize, db,dbtable);
			test.dht.setWriteBuffer(wrtBuffer);
			
			test.refreshfolder(parfolder);
		//test.resAlloc = new ConResAlloc(parfolder,symbol, parts, no_triple,test.mysql);
			Statistic sta = new Statistic();
		//test.nodebalance(test.parts_stmt_info);
				
		// test.resAlloc = new StickyResAlloc(parfolder, symbol, parts, no_triple, test.mysql);
			try{
				test.storeall(source);
				System.out.println("parts stmt info: ");
				test.status();
					
				int sumcut = sta.sumCut(RandomP.mysql);
				System.out.println("total cut number is: "+sumcut);
				sta.nodebalance(test.parts_stmt_info);
				test.mysql.closeConnection();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
}
