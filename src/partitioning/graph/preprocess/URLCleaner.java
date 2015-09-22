package preprocess;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.util.HashMap;

/**
 * clean the string, and shorten it with encode 
 * @author AI
 *
 *1, use URI prefix detecting appraoch to compress the URL (actually no need here, only MD5)
 *2, remove the special symbol out of URL
 *3, for really long URL (>= len(MD5)), encode it with MD5, 
 *4, if in MySql, wanna keep the value, save it in TEXT type with MD5 key as index
 */
public class URLCleaner {

	private String test = "<http://upload.wikimedia.org/wikipedia/commons/thumb/9/98/" +
			"100222_%25E5%258D%2597%25E6%258A%2595%25E5%258E%25BF_" +
			"%25E7%25AB%25B9%25E5%25B1%25B1%25E9%2595%2587_" +
			"%25E7%25A7%2580%25E6%259E%2597%25E9%2587%258C_" +
			"%25E9%25BB%2583%25E5%25B8%25B6%25E6%259E%259D%25E5%25B0%25BA%25E8%259B%25BE_" +
			"Milionia_basalis_pryeri_Druce%2C_1888_Moth_in_Chushan%2C_Taiwan.jpg/200px-100222_" +
			"%25E5%258D%2597%25E6%258A%2595%25E5%258E%25BF_" +
			"%25E7%25AB%25B9%25E5%25B1%25B1%25E9%2595%2587_" +
			"%25E7%25A7%2580%25E6%259E%2597%25E9%2587%258C_" +
			"%25E9%25BB%2583%25E5%25B8%25B6%25E6%259E%259D%25E5%25B0%25BA%25E8%259B%25BE_" +
			"Milionia_basalis_pryeri_Druce%2C_1888_Moth_in_Chushan%2C_Taiwan.jpg>";
	
	
	private HashMap<String, Integer> table = new HashMap<String, Integer>(10);
	private int count = 0;
	
	public String extractHost(String str){
		int len = str.length();
		try{
			URL u = new URL(str.substring(1, len-1));
			String host = u.getHost();
			System.out.println(host);
			if(!table.containsKey(host)){
				table.put(host, count);
				count++;
			}
			return host;	
		}catch(MalformedURLException e){
			e.printStackTrace();
			return null;
		}
	}
	
	public String extractPath(String str){
		int len = str.length();
		try{
			URL u = new URL(str.substring(1, len-1));
			return u.getPath();
			
		}catch(MalformedURLException e){
			e.printStackTrace();
			return null;
		}
	}
	
	public String extractProtocol(String str){
		int len = str.length();
		try{
			URL u = new URL(str.substring(1, len-1));
			return u.getProtocol();
			
		}catch(MalformedURLException e){
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * turn it to MD5 key
	 * @param str : URL
	 */
	public String transform(String str){
		int ord = table.get(extractHost(str));
		try{
			byte[] input = str.getBytes("UTF-8");
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.reset();
			byte[] output = md.digest(input);
			StringBuffer key = new StringBuffer();
			key.append(ord).append(':');
			for (int i = 0; i < output.length; ++i) {
		          key.append(Integer.toHexString((output[i] & 0xFF) | 0x100).substring(1,3));
		    }
			return key.toString();
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * remove all special symbol from URL String
	 * @param str: URL
	 * @return
	 */
	public String clean(String str){
		String tar = str.replaceAll("[^a-zA-Z0-9:/%<>.]", "");
		// tar.replaceAll("[ ]+", ""); usually not common in URL string
		return tar;	
	}
	

	public static void main(String[] args){
		URLCleaner te = new URLCleaner();
		te.extractPath(te.test);
		String md5 = te.transform(te.test);
		System.out.println(md5);
		String clean = te.clean(te.test);
		md5 = te.transform(clean);
		System.out.println(md5);
	}
}
