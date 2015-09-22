package SPar.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Set;

import SPar.struct.Quad;

/**
 * Mysql DB connection and related operations
 * @author AI
 *
 */

public class DBOperation {
	/*
	 * fixed environment, no change for user, server IP
	 * so this server connection fixed.
	 */
	private static String user = "ray";
	private static String pwd = "123321/wr";
	public static Connection conn = null;
	private static String url = "jdbc:mysql://localhost:"; //3666 jdbc:mysql://saturn.cs.binghamton.edu:3306/
	private static String table;
	
	public DBOperation(String db,String rtable, String port){
		url = url + port+"/"+db;
		table = rtable; 
		connect();
	}
	
	public Connection getConnct(){
		return conn;
	}
	
	public void connect()
	{
		
		//String user= "nsheth2"; //"root";
        //String pwd= "lspa_mysql#"; //"";
        try{
        	Class.forName("com.mysql.jdbc.Driver").newInstance();
        	conn = DriverManager.getConnection(url,user,pwd);
        	System.out.println("Connected...");
        }
        
        catch(Exception se){
        	se.printStackTrace();
        }
	}
	
	public void closeConnection(){
		try{
		   conn.close();
		}catch(SQLException se){
			   se.printStackTrace();
		}
	}

	/**
	* execute sql
	* @return ArrayList<Quad> result set for certain SQL
	*/
	public ResultSet search(String sql){
		PreparedStatement prepstmt = null;
		ResultSet rs = null;
//		ArrayList<Quad> rset = new ArrayList<Quad>();
		try {
			prepstmt = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs = prepstmt.executeQuery();
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	* Insert query generator
	* @return String sql query
	*
	public String insertQ(Quad ind_item){
		StringBuilder sql = new StringBuilder("insert into `index` values('");
		sql.append(ind_item.resource).append("','").append(ind_item.part).append("','").append(ind_item.type)
		.append("','").append(ind_item.weight).append("')");
		return sql.toString();
	}*/
	
	/**
	* delete query generator
	* @return String sql query
	*/
	public String deleteQ(String resource, int part){
		if(resource.indexOf('\'') != -1 )
			resource = resource.replaceAll("'", "''");
	//	resource = String.valueOf(resource.hashCode());
		StringBuilder delsql = new StringBuilder("delete from `"+table+"` where ");
		delsql.append("resource='").append(resource).append("' and part=").append(part);
		return delsql.toString();
	}
	
	public boolean deleteAll(){
		String  delsql = "delete from `"+table+"`";
		return updateSql(delsql);
	}
	
	/**
	* update res index
	* @return boolean
	*/
	public boolean updateSql(String sql){
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			if(ps.executeUpdate() > 0) return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.print(sql);
			return false;
		} finally{
			if (ps != null) {
                try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
		}
		return false;
	}

/**
 *  index search operation: 
 *  @return HashSet<Integer> list of parts include the resource
 */
	public Hashtable<Integer,Quad> searchIndex(String resource){
		if(resource.indexOf('\'') != -1 )
			resource = resource.replaceAll("'", "''");
		// use hashcode to seal the string detail, also better for string processing without special char annoy
		//resource = String.valueOf(resource.hashCode()); 
		
		String sql = "select type_s,type_o,part,weight,o_bytes,s_bytes from `"+table+"` where resource='"+resource+"'";
		ResultSet rs = null;
		PreparedStatement prepstmt = null;
		Hashtable<Integer,Quad> partlist = new Hashtable<Integer,Quad>();
		Quad record;
		
		try {
			prepstmt = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);
			rs = prepstmt.executeQuery();
			int part;
			while(rs.next()){
				record =new Quad();
				part = Integer.parseInt(rs.getString("part"));
				record.type_s = rs.getInt("type_s");
				record.type_o = rs.getInt("type_o");
				record.weight = rs.getInt("weight");
				record.o_bytes = rs.getInt("o_bytes");
				record.s_bytes = rs.getInt("s_bytes");
				partlist.put(part,record);
			}
			return partlist;
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally{
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
		return null;
	}
	
/**
 * createIndex: two plan to realize, first use insert and update for each item; second
 * use update for the existed records, if not existed (assume know it quick), put it into a file
 * and use load file for those unexsited records. (to see which one is faster)
 * @param ind_buffer
 * @return Boolean
 */
	
	public boolean createIndex(Hashtable<String, Hashtable<Integer,Quad>> ind_buffer){
		Set<String> keys = ind_buffer.keySet();
		Hashtable<Integer,Quad> list;
		Quad comp;
		int part;
		int type1_s, type1_o;
		int type_s,type_o;
		int wei, wei1; // compared use
		int o_bytes, s_bytes, o_bytes1, s_bytes1;
		String check = "select resource, part, type_s, type_o, weight, o_bytes, s_bytes from `"+table+"` where ";
		//String insert = "insert into `sindex` values ";
//		String update = "update "
		ResultSet rs = null;
		PreparedStatement prepstmt = null;
		String res;
		for(String key : keys){
			list = ind_buffer.get(key);
			if(key.indexOf('\'') != -1 )
				res = key.replaceAll("'", "''");
			else res = key;
		// hashcode sealing	
		//	key = String.valueOf(key.hashCode()); 
			
			for(int i : list.keySet()){
				comp = list.get(i);
				part = i;
				type_s = comp.type_s;
				type_o = comp.type_o;
				wei = comp.weight;
				o_bytes = comp.o_bytes;
				s_bytes = comp.s_bytes;
			// seach if have res in table
				StringBuilder sql = new StringBuilder();
	
				sql.append(check).append("resource='").append(res).append("' and part=").append(part);

				try {
					prepstmt = conn.prepareStatement(sql.toString(),ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					rs = prepstmt.executeQuery();
					if(rs.next()){
						// updates the records	
						type1_o = rs.getInt("type_o");
						type1_s = rs.getInt("type_s");
						wei1 = rs.getInt("weight");
						o_bytes1 = rs.getInt("o_bytes");
						s_bytes1 = rs.getInt("s_bytes");
						// unpdate records	
						wei += wei1;
						/*if(wei < wei1){
							wei = wei1;
							flag2 = 1;
						}*/
						
						o_bytes1 += o_bytes;
						s_bytes1 += s_bytes;
						if(type_s != 0 && type1_s == 0)
							rs.updateInt("type_s", type_s);
						if(type_o != 0 && type1_o == 0)
							rs.updateInt("type_o", type_o);
					//	if(flag2 == 1)
							rs.updateInt("weight", wei);
						rs.updateInt("o_bytes", o_bytes1);
						rs.updateInt("s_bytes", s_bytes1);
						
						rs.updateRow();
					}else {
				// directly insert the record
					
				/**		(option 1 as below)	
				 *		value = "('"+key+"',"+part+",'"+type+"',"+wei+")";
				 *		updateSql(insert+value);
				 */
				//		option 2 to realize		
						rs.moveToInsertRow();
						rs.updateString("resource", key);
						rs.updateInt("part", part);
						rs.updateInt("type_s", type_s);
						rs.updateInt("type_o", type_o);
						rs.updateInt("weight", wei);
						rs.updateInt("o_bytes", o_bytes);
						rs.updateInt("s_bytes", s_bytes);
						rs.insertRow();
						
					}
				} catch (SQLException e) {
					e.printStackTrace();
					System.out.println(key);
				}finally{
				// ??should wait until all database operation finished!		
					if (rs != null) {
						try {
							rs.close();
							prepstmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		return true;
	}
	
/**
 * the second version of create index for random partitioning
 */ 
	
	public boolean createIndex_random(Hashtable<String, Hashtable<Integer,Quad>> ind_buffer){
		Set<String> keys = ind_buffer.keySet();
		Hashtable<Integer,Quad> list;
		int part;
		
		int type_s, type_o, type1_s, type1_o;
		int o_bytes, s_bytes, o_bytes1, s_bytes1;
		
		String check = "select resource, part, type_s, type_o, weight, o_bytes, s_bytes from `"+table+"` where ";
		ResultSet rs = null;
		PreparedStatement prepstmt = null;
		Quad item;
		String res;
		for(String key : keys){
			list = ind_buffer.get(key);
			if(key.indexOf('\'') != -1 )
				res = key.replaceAll("'", "''");
			else
				res = key;
			for(int i : list.keySet()){
				item = list.get(i);
				type_s = item.type_s;
				type_o = item.type_o;
				
				o_bytes = item.o_bytes;
				s_bytes = item.s_bytes;
				part = i;
	
			// seach if have res in table
				StringBuilder sql = new StringBuilder();
			
				sql.append(check).append("resource='").append(res).append("' and part=").append(part);
			//	rs = search(sql.toString());
				try {
					prepstmt = conn.prepareStatement(sql.toString(),ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
					rs = prepstmt.executeQuery();
					if(rs.next()){
					// updates the records	
						type1_s = rs.getInt("type_s");
						type1_o = rs.getInt("type_o");
						o_bytes1 = rs.getInt("o_bytes");
						s_bytes1 = rs.getInt("s_bytes");
						
						o_bytes1 += o_bytes;
						s_bytes1 += s_bytes;
					// unpdate records		
						if(type_s != 0 && type1_s == 0)
							rs.updateInt("type_s", type_s);
						if(type_o != 0 && type1_o == 0)
							rs.updateInt("type_o", type_o);
						rs.updateInt("o_bytes", o_bytes1);
						rs.updateInt("s_bytes", s_bytes1);
						rs.updateRow();
					}else {
					// directly insert the record
						rs.moveToInsertRow();
						rs.updateString("resource", key);
						rs.updateInt("part", part);
						rs.updateInt("type_s", type_s);
						rs.updateInt("type_o", type_o);
						rs.updateInt("o_bytes", o_bytes);
						rs.updateInt("s_bytes", s_bytes);
						rs.insertRow();
					}
				} catch (SQLException e) {
					e.printStackTrace();
					return false;
				}finally{
					if (rs != null) {
						try {
							rs.close();
							prepstmt.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		return true;
	}

/**
 * running test
 * @param args
 *
	public static void main(String[] args){
		String table = "gindex";
		DBOperation dbo = new DBOperation(table);
		String resource = "<http://www.w3.org/2001/sw/RDFCore/ntriples/>";
		int part = 1;
		String sql = dbo.deleteQ(resource, part);
		if(dbo.updateSql(sql)) System.out.println("success!");
		dbo.closeConnection();
	}
	*/
}