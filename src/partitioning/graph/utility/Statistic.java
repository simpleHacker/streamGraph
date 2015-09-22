package SPar.utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * this class is for summing up the edge cut info
 * for the whole partition. It also calculate the size load
 * balance over all partitions
 */
public class Statistic {
	
	public ResultSet search(String sql, Connection con){
		PreparedStatement prepstmt = null;
		ResultSet rs = null;

		try {
			prepstmt = con.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,
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
 * operate on mysql table, and visit every resource in table for their total appearance in No. of partition	
 * if a resource appear more one times in table, mean it have a cut, just sumup all records in table, 
 * then minus total number of resource, you get the total cut	
 * @param mysql
 * @return
 */
	public int sumCut(DBOperation mysql){
		
		String resquery = "select count(resource) from `sindex`";
		String no_res = "select count(distinct resource) from `sindex`";
		
		ResultSet rs = search(resquery, mysql.getConnct()); 
		ResultSet rs_no = search(no_res, mysql.getConnct());
		
		int sumup;
		int base;
		try{
			if(rs.next()){
				
				sumup = rs.getInt("count(resource)");
				rs_no.next();
				base = rs_no.getInt("count(distinct resource)");
				System.out.println("total resource number: "+base);
				return sumup-base;
			} else 
				return -1;
		} catch(SQLException e){
			e.printStackTrace();
			return -1;
		}
	}
	
/**
 * calculate the size deviation of all paritions for evaluation of size balance of partitions
 * @param parts_stmt_info
 */	
	public static void nodebalance(int[] parts_stmt_info){
		double total=0;
		int size = parts_stmt_info.length;
		int sum = 0;
		for(int i = 0;i<size;++i){
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
	
}
