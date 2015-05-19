package SPar;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import SPar.struct.SNComb;
import SPar.utility.DBOperation;
import SPar.utility.Localinfo;

public class Retrival {
	
	public final int capacity = 100000; 
	private float factor = 0.90F;
	
	private static String symbol; 
	private static int[] workloads; // sumup query No handled by each partition
	private static int[] workloads_gp; 
	private static int[] workloads_dht;
	
	public DBOperation mysql;
//	private static String db;
//	private static String table;
	
	public Retrival(String symbol,int nparts, String db, String dbtable, String port){
		this.symbol = symbol;
		workloads = new int[nparts];
		workloads_gp = new int[nparts];
		workloads_dht = new int[nparts];
		
		for(int i=0;i<nparts;++i){
			workloads[i] = 0;
			workloads_gp[i] = 0;
			workloads_dht[i] = 0;
		}
		this.mysql = new DBOperation(db, dbtable, port);
	}

/**  pattern 2 query, workload and No. of part statistic, no join */
	public HashSet<Integer> res_single(String res,String type){
		String query;
		if(res.indexOf('\'') != -1 )
			res = res.replaceAll("'", "''");
	// hash code sealing	
	//	res = String.valueOf(res.hashCode()); 
		if(type.equals("s"))
			query = "select part from `sindex` where resource='"+res+"' and type_s = 1";
		else
			query = "select part from `sindex` where resource='"+res+"' and type_o = 1";
		ResultSet rs = null; //mysql.search(query);	
		PreparedStatement prepstmt = null;
		HashSet<Integer> partlist = new HashSet<Integer>();
		int  part;
		try {
			prepstmt = mysql.conn.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs = prepstmt.executeQuery();
			while(rs.next()){
				part = rs.getInt("part");
				partlist.add(part);
				workloads[part]++;
			}
		} catch(SQLException e){
			e.printStackTrace();
			return null;
		}finally{
			if (rs != null) {
				try {
					rs.close();
					prepstmt.close();
				} catch (SQLException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return partlist;
	}
	
	public int res_single_(String res, HashSet<Integer> partlist, String type,HashSet<Integer> supparts, int flag){
				
		String query;
		if(res.indexOf('\'') != -1 )
			res = res.replaceAll("'", "''");
		
		//res = String.valueOf(res.hashCode()); 
		if(type.equals("s"))
			query = "select part from `sindex` where resource='"+res+"' and type_s = 1";
		else
			query = "select part from `sindex` where resource='"+res+"' and type_o = 1";
		ResultSet rs = null; //mysql.search(query);
		PreparedStatement prepstmt = null;
		int parts = 0, part;
		
		try {
			prepstmt = mysql.conn.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs = prepstmt.executeQuery();
			while(rs.next()){
				part = rs.getInt("part");
			/**  
			 * recognize method purpose for different targets dht: flag = 1; gp: flag = 0;
			 * if it is DHT, no need to account its supper query part, because it is a totally new querying
			 */															
				if(flag == 1) parts++;
				else if(!supparts.contains(part)) parts++;
				partlist.add(part);
			}
		} catch(SQLException e){
			e.printStackTrace();
			partlist = null;
		}finally{
			if (rs != null) {
				try {
					rs.close();
					prepstmt.close();
				} catch (SQLException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return parts;
	}
	
/**  
 * batch processing: put query res in list group by its part (hashtable structure)
 * Version 1 for non-double chain query
 * @param res
 * @param type
 * @param part_res
 * @return
 */
	public int allocater(String res,String type,Hashtable<Integer,ArrayList<String>> part_res){
		/* test index completeness   !!!!!!!!!!!!!!!!!
				if(!index.containsKey(res)){
					System.err.println("allocater-res: "+res);
					System.exit(1);
				} */
			int size = 0;
			String resource;
			//  list show different part res is in and its related info.	
			if(res.indexOf('\'') != -1 )
				resource = res.replaceAll("'", "''");
			else 
				resource = res;
				//String resource = String.valueOf(res.hashCode()); 
			String query = "select part, type_s, type_o from `sindex` where resource='"+resource+"'";
			ResultSet rs = null; // mysql.search(query);
			PreparedStatement prepstmt = null;
			int part;
			int types, typeo;
			ArrayList<String> reslist;
			try{
				prepstmt = mysql.conn.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				rs = prepstmt.executeQuery();
				while(rs.next()){
					part = rs.getInt("part");
					types = rs.getInt("type_s");
					typeo = rs.getInt("type_o");
			// type = "o"/"s"	every res will be different
					if((type.equals("s") && types == 1) || (type.equals("o") && typeo == 1)){
						size += 1;
						workloads[part]++;
						if(!part_res.containsKey(part)){
							reslist = new ArrayList<String>();
						}else reslist = part_res.get(part);
						reslist.add(res);
						part_res.put(part, reslist);
						
					}
				}
			} catch (SQLException e){
				e.printStackTrace();
			}finally{
				if (rs != null) {
					try {
						rs.close();
						prepstmt.close();
					} catch (SQLException e) {
					// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			return size;
	}	
/**
 * double chain 2 query processing and statistic functions
 * _allocater: gather sub-query res info into correspondent part list (table structure)
 * _disseminater: batch processing sub-queries with each type res in each partition
 * batcher: batching up the processing
 * 
 */	
	
// !! optional allocater, also act as a part collector of what res sits in for a filter reference of local index of the 
// another part query. But different for chain query, need more information (res-queryNo-binded_parts-)
// [part: list<queryNo:portion:res>]; binded_parts	
	
	class QueryInfo{
		int queryNo;
		String res;
		int subpart;
	}
	public HashSet<Integer> _allocater(String res,int queryNo,String type,Hashtable<Integer,ArrayList<QueryInfo>> part_res, int subpart){

		HashSet<Integer> collector = new HashSet<Integer>();
	//  list show different part res is in and its related infos.
		String resource;
		if(res.indexOf('\'') != -1 )
			resource = res.replaceAll("'", "''");
		else 
			resource = res;
	//	String resource = String.valueOf(res.hashCode()); 
		String query = "select part, type_s, type_o from `sindex` where resource='"+resource+"'";
		ResultSet rs = null; //mysql.search(query);
		PreparedStatement prepstmt = null;
		
		int part;
		int types, typeo;
		ArrayList<QueryInfo> reslist;
		try{
			prepstmt = mysql.conn.prepareStatement(query,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs = prepstmt.executeQuery();
			while(rs.next()){
				part = Integer.parseInt(rs.getString("part"));
				types = rs.getInt("type_s");
				typeo = rs.getInt("type_o");
				// type = "o"/"s"	every res will be different
				if((type.equals("s") && types == 1) || (type.equals("o") && typeo == 1)){
				// target to one sub query, sum how many part it send mess to	
					collector.add(part); 

					workloads[part]++;
					if(!part_res.containsKey(part)){
						reslist = new ArrayList<QueryInfo>();
					}else reslist = part_res.get(part);
					QueryInfo q = new QueryInfo();
					q.queryNo = queryNo;
					q.res = res;
					q.subpart = subpart; // use this to tell which part this subquery belongs to.
	
					reslist.add(q);
					part_res.put(part, reslist);
					if(type.equals("s")) break;
				}
			}
			return collector;
		} catch (SQLException e){
			e.printStackTrace();
		}finally{
			if (rs != null) {
				try {
					rs.close();
					prepstmt.close();
				} catch (SQLException e) {
				// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return collector;
	}

// {queryNo, bytes}	and part_res is already classified before this operation
	public long _disseminater(Hashtable<Integer,ArrayList<QueryInfo>> part_query,String folder, String type, ArrayList<Hashtable<Integer,Integer>> sumer){
		Set<Integer> list = part_query.keySet();
		long sum = 0; // non-filted bytes statistic
		ArrayList<QueryInfo> reslist;
	//@semantics: table<res,table<nei,bytes>> below
		Hashtable<String, Hashtable<String,Integer>> dlist; // inlist/outlist
	//@semantics: list:<part, list:type=resource>	below
		Hashtable<Integer,HashSet<String>> Lindex;	
		for(int part : list){
			reslist = part_query.get(part);
			String filepath = folder+symbol+"partition_"+part;
			Lindex = new Hashtable<Integer, HashSet<String>>();
			if(type.equals("s")){
				// create Lindex with sub type res entry
				dlist = Localinfo._outlinkinfo(filepath,mysql,Lindex, part);
				// batcher to sum up locally filtered intermediate result bytes for sub res part query
				sum  = batcher(-1,dlist,sumer,Lindex,reslist); 
				// dlist filtered with sub in other part entry (sub_part_res)
			}
			else{ 
				// create Lindex with obj entry
				dlist = Localinfo._inlinkinfo(filepath,mysql,Lindex,part);
				// batcher to sum up locally filtered intermediate result bytes for obj res part query
				sum = batcher(part,dlist,sumer,Lindex,reslist);
				// dlist filtered with obj in other part entry (obj_part_res)
			}
	
		
		}
		return sum; // non-filtered bytes sumup
	}
	
/* @design idea
 * need to union the HashSet of lindex of a part; Lindex : type=resource
 * new type Lindex design: type-Hashtable<res,HashSet<part>>, best have two type Lindex for each partition
 * this structure is better to check for filtered intermediate result first, then check 
 * when subpart = -1, it is subpart query bytes sumer, if != -1, it is objpart query bytes sumer, use subpart to filter result	
 */ 
/** 
 * batcher is used for summing up the bytes for each query, 
 * it return batch info for query-based bytes		
 */
	public long batcher(int partition,Hashtable<String, Hashtable<String,Integer>> dlist, ArrayList<Hashtable<Integer,Integer>> sumer, // queryNo based list
			Hashtable<Integer,HashSet<String>> Lindex, ArrayList<QueryInfo> querylist){
		Set<String> neilist; 
		Set<Integer> entry = Lindex.keySet();
		Hashtable<Integer, Integer> counter; // <part,sum_bytes>
		
		Hashtable<String,Integer> neistruct; // <neires, rest_bytes>
		QueryInfo q;
		long sumsum = 0;
		HashSet<String> retain;
		for(int i=0;i< querylist.size();++i){
		// get neis of res of each query
			q = querylist.get(i);
			neistruct = dlist.get(q.res);
			neilist = neistruct.keySet();
			
		// filtered with local index and return screened nei list as intermediate results for a query
			if(partition == -1){ // for sub type part chain query case, only been calculated once!
				counter = new Hashtable<Integer,Integer>();
				for(int part : entry){
					int sum = 0;
					HashSet<String> tmp = new HashSet<String>(neilist);
					retain = Lindex.get(part);
					if(retain != null)
					/** list of nei res as intermediate result need to be send out to other parts for join,
					 * intersection with Lindex for part
					 */	
						tmp.retainAll(retain); 
					else
						continue;
				// sum up the bytes for filtered intermediate results, also record bytes for queries sending to each part 
					for(String nei: tmp){
						sum += neistruct.get(nei); // bytes for filtered intermediate results
					}
					if(sum != 0){
						counter.put(part, sum);
					}
				}
			}else{ // for obj type part chain query case, sent to many parts, calculated multi-time
				int sum = 0;
				HashSet<String> tmp = new HashSet<String>(neilist);
				if(sumer.get(q.queryNo) != null)
					counter = sumer.get(q.queryNo);
				else
					counter = new Hashtable<Integer,Integer>();
				retain = Lindex.get(q.subpart);
				if(retain != null){
					tmp.retainAll(retain); 
				/** sum up the bytes for filtered intermediate results,
				 *  also record result's bytes of query for each part */
					for(String nei: tmp){
						sum += neistruct.get(nei);
					}	
					if(sum != 0){
						counter.put(partition, sum); // sub-query bytes statistic in each part only been calculated once
					}
				}
			}
		/** this sumup is for non-filtered strategy, 
		 * just find all intermediate result bytes.
		 * ??any smart options to do this*/	
			
			for(String nei: neilist){
				sumsum += neistruct.get(nei);
			}
//			Test Code
//			System.out.println("query no: "+q.queryNo+' '+sumer.size());
			
			sumer.set(q.queryNo,counter); // sumer struct is seperated for sub type query part and obj type query part
		
		/**	Algorithm for sum up intermedate results for each chain query 
		 * for sub type query part, get all the bytes need to be sent to each part-Hashtable<part,bytes>
		 * for obj type query part, only get all the bytes need to be sent to sub type res part-one part
		 * then for each query compare the bytes bwteen its two sub query parts on each partition, 
		 * and sum up to the final total bytes with the compared smaller one
		*/
		}
		return sumsum;
	}
	
	// sum up the CC for pattern 3 and 4 queries
		public int disseminater(Hashtable<Integer,ArrayList<String>> part_res, String folder, String type, int flag){
			Set<Integer> list = part_res.keySet();
			int sum = 0;
			ArrayList<String> reslist;
			Hashtable<String,SNComb> dlist;
			int[] qsize = new int[2];
			int qbytes = 0;// qbytes denotes all bytes for querying messages
			long dbytes = 0;// dbytes denotes all bytes for intermediate result transfer 
			for(int part : list){
	//@test
	//			System.err.println("part "+part);
				
				reslist = part_res.get(part);
				String filepath = folder+symbol+"partition_"+part;
				if(type.equals("s"))
					dlist = Localinfo.outlinkinfo(filepath); 
				else 
					dlist = Localinfo.inlinkinfo(filepath);
				sum = sum + res_path(reslist,dlist,type,qsize,flag); // all parts' statistics in querying
				qbytes += qsize[0];
				dbytes += qsize[1];
			}
			System.out.println("Second round type "+type+" composite querys' message bytes: "+qbytes);
			System.out.println("Type "+type+" Intermediate result transfer bytes: "+dbytes);
			return sum;
		}

	// resolve pattern 3 and 4 queries, get input from the collections made by allocator()	
		public int res_path(ArrayList<String> res, Hashtable<String,SNComb> dlist, String type, int[] qsize, int flag){
			
			HashSet<String> nei;
			int qbytes = 0, dbytes = 0, querysize, parts;
			HashSet<Integer> partlist = new HashSet<Integer>(); 
			HashSet<Integer> oparts;
			int sum = 0;
			for(String r : res){
				
	//	@test
	//			if(!dlist.containsKey(r))
	//				System.err.println("Dlist error without certain entry!!! "+r);
	/**
	 * to the case, r only has literal object, so it shouldn't appears in 
	 * results of chain query, dlist only contain the resource appears in no-literal 
	 * object triples
	 */
				if(dlist.containsKey(r)){
					nei = dlist.get(r).nlist;
				// sum up the size of intermediate result
					dbytes += dlist.get(r).dsize;
				
					oparts = res_single(r,type); // for all parts, "r" takes "type" 
					for(String n : nei){
				// secondary sub-query statistic:
						querysize = querysizer(n);
				/** considered the subquery sends to the same part, 
				 * then not count it for more times,
				 */				
						parts = res_single_(n,partlist,type,oparts,flag);
						qbytes = qbytes + querysize*parts;
					}
					partlist.addAll(oparts);
					sum = sum + partlist.size();
					partlist.clear();
				}
			}
			qsize[0] = qbytes;
			qsize[1] = dbytes;
			return sum;
		}
		
	//  calculate the query size for aggrasive query pattern	
		public static int querysizer(String fill){
			String qbody = "SELECT ?p ?x WHERE { ?x ?p }";
			return qbody.length()+ fill.length();
		}
		
	//  how many patterns has been known, just for switch purpose	
		public static String[] queryparser(String query){
			String[] reg = new String[2];
			String[] vars = null;
			String[] patterns;
			int select = query.toLowerCase().indexOf("select");
			int where = query.toLowerCase().indexOf("where");
			String first = query.substring(select, where);
			String second = query.substring(where);
			int index;
			if((index=first.indexOf("?")) != -1){
				first = first.substring(index);
				vars = first.split(" ");
				
//				@test
//				System.out.println("var length: "+vars.length);
//				System.out.println("vars: "+first);
			}else
				return null;
			
			second = second.substring(second.indexOf("{")+1, second.indexOf("}"));
			second = second.trim(); // remove tails for each query pattern
			patterns = second.split(" .\\n"); 
			String type;
			if(patterns.length > 1) type = "2";
			else type = "1";
		
			for(int i=0;i<patterns.length;++i){
				patterns[i] = patterns[i].trim();
				String[] items = patterns[i].split(" ");
				if(vars.length == 3){
					if(items[0].charAt(0) != '?'){
						reg[0] = type+"so="+items[0];
						continue;
					}else{
						reg[1] = items[2];
						return reg;
					}
				}else{
					if(items[0].charAt(0) != '?'){
						reg[0] = type+"s="+items[0];
						reg[1] = null;
						return reg;
					}
					if(items[2].charAt(0) != '?'){
						reg[0] = type+"o="+items[2];
						reg[1] = null;
						return reg;
					}
				}
			}
			return null;
		}
		
	/** 
	 * query executing simulator
	 * sum total No. of query messages and its bytes in benchmark query set processing
	 * sum up bytes for intermediate results during query processing
	 * sum up the workload of each partition
	 * 
	 * @param querys
	 * @param folder
	 * @param parts
	 * @param flag
	 * @return
	 */
		public int queryExecute(ArrayList<String> querys,String folder,int parts, int flag){
		
			int statistic = 0; // how many query messages involved in quering
			int s1 = 0, s2 = 0, s3 = 0, s4 = 0, s5 = 0;
			int qnum = querys.size();
			int qbytes = 0; //  all bytes for querying messages	
		
			
// @test 	
//			System.out.println("global res size:"+index.size());
		// sub-query info, like res, collector strucuture
			Hashtable<Integer,ArrayList<String>> part_objs = new Hashtable<Integer,ArrayList<String>>(capacity,factor);
			Hashtable<Integer,ArrayList<String>> part_subs = new Hashtable<Integer,ArrayList<String>>(capacity,factor);
			Hashtable<Integer,ArrayList<QueryInfo>> part_csubs = new Hashtable<Integer,ArrayList<QueryInfo>>(capacity,factor);
			Hashtable<Integer,ArrayList<QueryInfo>> part_cobjs = new Hashtable<Integer,ArrayList<QueryInfo>>(capacity,factor);
			
			int query2num = 0,query3num = 0, query4num = 0, query5num = 0;
			
			System.out.println("Query execute result:");
			String query;
		 
			int p = -1; // case checker for different kind of query pattern
			
			for(int q = 0; q<qnum;++q){
				
				query = querys.get(q);
				// get query as : type=res	
				
				String[] key = queryparser(query);
//		 @test	
//				System.out.println("key: "+key);
						
				int pos = key[0].indexOf("=");
				String pattern = key[0].substring(0, pos);
				String res = key[0].substring(pos+1);
				String subres=null, objres=null;
				int queryNo = q;
//		@test	
//			System.out.println("patterns: "+pattern+" res: "+res);
						
					// for double side chain query, it should only have one case p=5, but two processing.
						
				if(pattern.equals("1s")) p = 1;
				else if(pattern.equals("1o")) p = 2;
				else if(pattern.equals("2s")) p = 3;
				else if(pattern.equals("2o")) p = 4;
				else if(pattern.equals("2so")){
					p = 5;
					subres = res;
					objres = key[1];
				}
				
//				 fault telorant querying, print problem query, and position its problem
				try{
				
					switch(p){
				//qbytes used to sum up how many bytes in querying messages in total
						case 1 : int sizes = res_single(res,"s").size(); qbytes = qbytes + query.length()*sizes; statistic = statistic + sizes; s1 += sizes;
							break;
						case 2 : int sizeo = res_single(res,"o").size(); qbytes = qbytes + query.length()*sizeo; statistic = statistic + sizeo; s2 += sizeo; query2num++;
							break;
						case 3 : int sizess = allocater(res,"s",part_subs);qbytes = qbytes + query.length()*sizess; query3num++;
							break;
						case 4 : int sizeoo = allocater(res,"o",part_objs);qbytes = qbytes + query.length()*sizeoo; query4num++;
							break;
						case 5 :{ 
				//@warning: subres, objres, subpart cannot be null, also limited to common sub storage		
							HashSet<Integer> spc = _allocater(subres,queryNo,"s",part_csubs,-1);qbytes = qbytes + querysizer(res);
							int subpart = -1;
							for(int part : spc){
								subpart = part;
								break;
							}
							HashSet<Integer> opc = _allocater(objres,queryNo,"o",part_cobjs,subpart);qbytes = qbytes + (querysizer(objres))*opc.size();
							opc.addAll(spc);
							statistic += opc.size(); // all parts involved in querying
							s5 += opc.size();	
							query5num++;
							break;}
						default : System.err.println("pattern wrong");
					}
				} catch(Exception e){
					System.err.println("!problem query resouce: "+res+"; type: "+p);
					e.printStackTrace();
				}
			}
			
				Runtime runtime = Runtime.getRuntime();
				if(p != 5){
					System.out.println("First round query message bytes: "+qbytes);
			
		     //	sum up pattern 3 query CC
					int size = disseminater(part_subs,folder,"s",flag);
					statistic  =statistic + size;
					s3 += size;
		     // sum up pattern 4 query CC	
					size = disseminater(part_objs,folder,"o",flag);
			//  memory usage report
					System.out.println("*Retrival Performance checker (below)");
					System.out.println("*Free memory: "+runtime.freeMemory());
					System.out.println("*Used memory: "+(runtime.totalMemory()-runtime.freeMemory()));
					System.out.println("***");
				
					statistic = statistic + size;
					s4 += size;
			
					System.out.println("<?s,?p,o> query no: "+query2num+"; num of message in querying: "+s2);
					System.out.println("<s,?p1,?x> <?x ?p2, ?y> query no: "+query3num+"; num of message in querying: "+s3);
					System.out.println("<?x, ?p1, ?y> <?y, ?p2, o> query no: "+query4num+"; num of message in querying: "+s4);
				}else{
		     //	sum up pattern 5 query, sub part CC, obj part CC seperately 
					long b5_non = 0;// non-filtered intermediate results byte
					ArrayList<Hashtable<Integer,Integer>> sub_sumer = new ArrayList<Hashtable<Integer,Integer>>(query5num);
					ArrayList<Hashtable<Integer,Integer>> obj_sumer = new ArrayList<Hashtable<Integer,Integer>>(query5num);
					for(int i=0;i<query5num;++i){
						sub_sumer.add(null);
						obj_sumer.add(null);
					}
				
					b5_non += _disseminater(part_csubs,folder,"s",sub_sumer);
			//  memory usage report
					System.out.println("*Retrival Performance checker (below)");
					System.out.println("*Free memory: "+runtime.freeMemory());
					System.out.println("*Used memory: "+(runtime.totalMemory()-runtime.freeMemory()));
					System.out.println("***");
				
					b5_non += _disseminater(part_cobjs,folder,"o",obj_sumer);
		    // 	integrate filtered sub part query bytes with filtered obj part query bytes 
					long b5 = integrate(sub_sumer,obj_sumer);

					System.out.println("<s,?p1,?x> <?x, ?p2, o> query no: "+query5num+"; num of message in querying: "+s5);
					System.out.println("chain query filtered Comm Cost in bytes: "+b5);
					System.out.println("chain query non-filtered conventional Comm Cost in bytes: "+b5_non);
				}
		//  workload of each partition
				System.out.println("Workload of each partition: (below)");
				for(int i=0;i<workloads.length;++i){
					System.out.println("partition "+i+": "+workloads[i]);
				}
				return statistic;
			
				
		}
		
/** 	
 * compare the intermediate results size from sub part query and obj part query
 * return the smaller part as transfered inter-result part and bytes
 * 
 * @param sub_sumer
 * @param obj_sumer
 * @return
 */	
		public long integrate(ArrayList<Hashtable<Integer,Integer>> sub_sumer, ArrayList<Hashtable<Integer,Integer>> obj_sumer){
			int size = sub_sumer.size();
			Hashtable<Integer,Integer> subq,objq;
			int sub_bytes, obj_bytes;
			long sum = 0;
//@test
//		if(size != obj_sumer.size())
//			System.err.println("sub part query distribution is different from obj par query!");
			
			for(int i=0;i<size;++i){
				subq = sub_sumer.get(i); // subq intermediate result size by parts
				objq = obj_sumer.get(i); // objq intermediate result size by parts
				for(int part : objq.keySet()){
					if(!subq.containsKey(part)) // no join of sub part query with obj part query
						continue; 
					sub_bytes = subq.get(part);
					obj_bytes = objq.get(part);
					if(sub_bytes > obj_bytes)
						sum += obj_bytes;
					else
						sum += sub_bytes;
				}
			}
			return sum;
		}
		
	
	/** read in query from query file, put in each type of query stack	*/
		public void queryReader(String queryfile, ArrayList<String> querylist){
			try{
				BufferedReader in = new BufferedReader(new FileReader(queryfile));
				String line=in.readLine();
				String query;
				while (line != null){
					if(line.charAt(0) == '+'){
						query = line.substring(1);
						while((line=in.readLine()) != null && line.charAt(0) != '+'){
							query = query+"\n"+line;
						}
						querylist.add(query);
// @test 
//				if(query.indexOf("<http://data.semanticweb.org/ns/swc/ontology#MealEvent>") != -1)
//					System.err.println(queryfile+"!!!!!!!!!!!!!!!!!!");				
					}
				}			
				in.close();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		
		public ArrayList<String> chainquerys(String folder){
			String queryfile = folder+symbol+"chain_query.que";
			ArrayList<String> querylist = new ArrayList<String>();
			queryReader(queryfile,querylist);
			return querylist;
		}
		
		public ArrayList<String> allquerys(String folder,int part){
			String queryfile;
			ArrayList<String> querylist = new ArrayList<String>();	
			for(int i=0;i<part;++i){
				queryfile = folder+symbol+"part_"+i+".que";
				queryReader(queryfile,querylist);
			}
	/* read in edge-cut query		
			queryfile = folder+"\\"+"edge_cut.que";
			queryReader(queryfile,querylist);*/
			return querylist;
		}
		
/*		public static void main(String[] args){
			
			String queryfolder = "GP_query_"+args[0];//args[0]
			String gp_parfolder = "GP_stmts_"+args[0];//args[0]
			String dht_parfolder = "DHT_stmts_"+args[0];
			//String cqueryfolder = "testpar";
			int parts = Integer.parseInt(args[0]);	// Integer.partInt(args[0]);
			
			Retrival rt = new Retrival("/",parts);
			
			int statistic;
/**		  flag denote which statistic is carried on (GP-0;DHT-1)	
 *		  when flag=0 for DHT, means DHT use coordinater free design		
 *			
*
			ArrayList<String> querylist = rt.allquerys(queryfolder, parts); // regular query
//			ArrayList<String> querylist = rt.chainquerys(cqueryfolder);
			int flag = 0;
			statistic = rt.queryExecute(querylist,gp_parfolder,parts,flag);
			//remeber also add in progress info in it	 
			// if can also add join cost infomation	 
			System.out.println("GP Query message number in querying: "+statistic);
			flag = 1;
			statistic = rt.queryExecute(querylist, dht_parfolder, parts,flag);
			System.out.println("DHT Query message number in querying: "+statistic);		 
			}*/

}
