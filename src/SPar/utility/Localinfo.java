package SPar.utility;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import SPar.struct.SNComb;


// Done:  also include the size info for transfer intermediate result for each resources.
// next modify Retrieval part to statistics all transfer size of one resources pattern 

/*
 * this class is for collect size information of stmt in a partition for a correpondant res
 * so it needs to read in all triples from a partition file, then store all information in a table
 */
//! no need for bytes information, its already in database s_bytes and o_bytes 

public class Localinfo {
	
//  intermediate result size added-in
	public static Hashtable<String,SNComb> outlinkinfo(String filename){
	//	System.out.println("part: "+filename);
		try{ 
			InputStream is = new FileInputStream(filename);
			NxParser nxp = new NxParser(is);
			SNComb c;
			Node[] ns;
			Node l;
// <resource, neighbor_list>
			Hashtable<String,SNComb> outlist = new Hashtable<String,SNComb>();
			while (nxp.hasNext()) {
				
				ns = nxp.next();
				String sub = ns[0].toN3();			
/*******************************test***************************
				if(sub.equals("<http://data.semanticweb.org/organization/boeing-phantom-works/location>"))
					System.out.println("I have outlist "+ns[2].toN3());
/**************************************************************/
				l = ns[2];
				if(!(l instanceof Literal)){
		
					String pro = ns[1].toN3();
					String obj = ns[2].toN3();
					int len = pro.length()+obj.length()+1;
					
					if(!outlist.containsKey(sub)){
						c = new SNComb(len);
					}else{
						c = outlist.get(sub);
						c.dsize = c.dsize+len;
					}
					if(!c.nlist.contains(obj)){
						c.nlist.add(obj); //!? this hope sub doesn't have so much obj
					}	
					outlist.put(sub, c);
				}
			}	
			return outlist;
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	} 
	
// !!optional outlink info with size info per neighbor (res, list<nei,bytes>)
// @extend with sub in other part Lindex creation. param:{DBConn, Lindex}
	public static Hashtable<String, Hashtable<String,Integer>> _outlinkinfo(String filename, DBOperation mysql, 
			Hashtable<Integer,HashSet<String>> index, int part){
		PreparedStatement prepstmt = null;
		ResultSet rs = null;
		try{
			InputStream is = new FileInputStream(filename);
			NxParser nxp = new NxParser(is,true);
			Hashtable<String, Hashtable<String,Integer>> outlist = new Hashtable<String, Hashtable<String,Integer>>();
			Hashtable<String,Integer> c;
			String sql;
			int p;
			HashSet<String> reslist;
			String resource;
			while (nxp.hasNext()) {
				Node[] ns = nxp.next();
				String sub = ns[0].toN3();				
				Node l = ns[2];
				if(!(l instanceof Literal)){
					String pro = ns[1].toN3();
					String obj = ns[2].toN3();		
					
					int len = pro.length()+obj.length()+1;
					// @ move to a certain block. create Lindex for obj filtering
					if(obj.indexOf('\'') != -1 )
						resource = obj.replaceAll("'", "''");
					else
						resource = obj;
					//resource = String.valueOf(obj.hashCode()); 
					sql = "select distinct part from `sindex` where resource='"+resource+"' and type_s = 1";
					prepstmt = mysql.conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					rs = prepstmt.executeQuery();
					
					while(rs.next()){
						p = rs.getInt("part");
						if(p != part){
							if (index.containsKey(p))
								reslist = index.get(p);
							else 
								reslist = new HashSet<String>(10);
							reslist.add(obj);
							index.put(p, reslist);
						}
					}// end Lindex creation 		
						
					rs.close();
					prepstmt.close();
					
					if(!outlist.containsKey(sub)){
						c = new Hashtable<String,Integer>();
					}else{
						c = outlist.get(sub);
						if(c.containsKey(obj))
							c.put(obj,c.get(obj)+len);
					}
					if(!c.containsKey(obj)){
						c.put(obj,len); //!? this hope sub doesn't have so much obj
					}	
					outlist.put(sub, c);
				}
			}	
			return outlist;
		}catch(Exception e){
			e.printStackTrace();
			try {
				rs.close();
				prepstmt.close();
			} catch (SQLException eq) {
			// TODO Auto-generated catch block
				eq.printStackTrace();
			}
			return null;
		}
	}
	
	public static Hashtable<String,SNComb> inlinkinfo(String filename){
		try{
			InputStream is = new FileInputStream(filename);
			NxParser nxp = new NxParser(is,true);
			SNComb c;
// <resource, neighbor_list>
			Hashtable<String,SNComb> inlist = new Hashtable<String,SNComb>();
			
			while (nxp.hasNext()) {
				Node[] ns = nxp.next();
				String sub = ns[0].toN3();				
				Node l = ns[2];
				if(!(l instanceof Literal)){
					
					String obj = ns[2].toN3();	
					String pro = ns[1].toN3();
					
/*******************************test***************************
					if(obj.equals("<http://data.semanticweb.org/organization/boeing-phantom-works/location>"))
						System.out.println("I have inlist");
/****************************************************************/					
					int len = pro.length()+sub.length()+1;	
					
					if(!inlist.containsKey(obj)){
						c = new SNComb(len);
					}else{
						c = inlist.get(obj);
						c.dsize = c.dsize+len;
					}
					if(!c.nlist.contains(sub)){
						c.nlist.add(sub);
					}
					inlist.put(obj, c);
				}
			}
			return inlist;
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	} 
	
// !! !!optional inlink info with size info per neighbor (res:table<nei,bytes>)
	public static Hashtable<String, Hashtable<String,Integer>> _inlinkinfo(String filename, DBOperation mysql,
			Hashtable<Integer,HashSet<String>> index, int part){
		
		PreparedStatement prepstmt = null;
		ResultSet rs = null;
		try{
			InputStream is = new FileInputStream(filename);
			NxParser nxp = new NxParser(is,true);
			Hashtable<String, Hashtable<String,Integer>> inlist = new Hashtable<String, Hashtable<String,Integer>>();
			
			int p;
			HashSet<String> reslist;
			String sql;
			Hashtable<String,Integer> c;
			String resource;
			while (nxp.hasNext()) {
				Node[] ns = nxp.next();
				String sub = ns[0].toN3();				
				Node l = ns[2];
				if(!(l instanceof Literal)){
					String pro = ns[1].toN3();
					String obj = ns[2].toN3();
					int len = pro.length()+sub.length()+1;
					
					// @ move to a certain block. create Lindex for sub filtering
					if(sub.indexOf('\'') != -1 )
						resource = sub.replaceAll("'", "''");
					else
						resource = sub;
					//resource = String.valueOf(sub.hashCode());
					sql = "select distinct part from `sindex` where resource='"+resource+"' and type_o = 1"; // !!!optimize the sql
					prepstmt = mysql.conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					rs = prepstmt.executeQuery();
					while(rs.next()){
						p = rs.getInt("part");
						if(p != part){
							if (index.containsKey(p))
								reslist = index.get(p);
							else 
								reslist = new HashSet<String>(10);
							reslist.add(obj);
							index.put(p, reslist);
						}
					}// end Lindex creation
					
					if(!inlist.containsKey(obj)){
						c = new Hashtable<String,Integer>();
					}else{
						c = inlist.get(obj);
						if(c.containsKey(sub))
							c.put(sub,c.get(sub)+len);
					}
					if(!c.containsKey(sub)){
						c.put(sub,len); //!? this hope sub doesn't have so much obj
					}	
					inlist.put(obj, c);
				}
			}	
			return inlist;
		}catch(Exception e){
			e.printStackTrace();
			if (rs != null) {
				try {
					rs.close();
					prepstmt.close();
				} catch (SQLException eq) {
				// TODO Auto-generated catch block
					eq.printStackTrace();
				}
			}
			return null;
		}
	}
	
	public static int sumup(Hashtable<String,Integer> nlist){
		int sum = 0;
		for (int b : nlist.values()) {
			sum += b;
		}
		return sum;
	}
}
