package SPar.allocation;

import SPar.utility.DBOperation;

public class AllocLoader {

	public static ResAlloc loadAlloc(String alloc, String parfolder,String symbol,int parts,float par_triple_no, DBOperation mysql) throws Exception{
		if(alloc.equals("consub"))
			return new ConResAlloc(parfolder,symbol, parts, par_triple_no,mysql);
		else if(alloc.equals("sticky"))
			return new StickyResAlloc(parfolder, symbol, parts, par_triple_no, mysql);
		else if(alloc.equals("smart"))
			return new SmartResAlloc(parfolder, symbol, parts, par_triple_no, mysql);
		else throw new Exception();
	}
}
