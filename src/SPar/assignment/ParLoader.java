package SPar.assignment;

// simple factory methods, if in future, more complicated, can change to factory patterm
public class ParLoader {
	public static ParAlgo loadPar(String par) throws Exception{
		if(par.equals("LRU"))
			return new LRU();
		else if(par.equals("LRUF"))
			return new LRUF();
		else if(par.equals("BG"))
			return new BalanceGreedy();
		else throw new Exception();
	}
}