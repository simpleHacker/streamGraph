package partitioning.graph.allocation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import partitioning.graph.partitioner.RandomP;
import partitioning.graph.struct.Part;
import partitioning.graph.struct.Quad;
import partitioning.graph.utility.DBOperation;
import partitioning.graph.utility.HashFunction;

public class DHT {
    private final HashFunction hashFunction;
    private final int numberOfReplicas; // can use real IP for replicas of each node
    public final SortedMap<BigInteger, Part> circle ;
    
//     public final int capacity = 10000; 
    public HashSet<String> resource_list;
    public HashSet<String> property_list;
     
//    node identity generation seed 
    private final String IPseed = "149.125.180.166";
    private final int portseed = 20;
    private final int regx = 13905;
//     private final int sizelimit = 1000;
    public String stmtfolder;
     
    private static String symbol = "/";

    static int sizelimit = 10000; // pending write for partitions
    static int INDEX_BUFFER_SIZE =  100000;
    DBOperation mysql;

/**
 * initial function to build the DHT network with num of nodes, with number of replicas
 * @param hashFunction
 * @param numberOfReplicas
 * @param numberOfNodes
 * @param parfolder
 * @param symbol
 */
    public DHT(HashFunction hashFunction, int numberOfReplicas,int numberOfNodes,String parfolder, String symbol) {
        this.symbol = symbol;
        this.hashFunction = hashFunction;
        this.numberOfReplicas = numberOfReplicas;
        this.circle = new TreeMap<BigInteger, Part>();
        this.stmtfolder = parfolder;
        Collection<Part> nodelist = createNodes(numberOfNodes);
        createDHT(nodelist);
        setParFolder(parfolder);
        try{
            File dirFile = new File(this.stmtfolder);
            if(!dirFile.exists() && !dirFile.isDirectory()){
                dirFile.mkdir();
            }
        } catch(Exception e){
             e.printStackTrace();
         }
     }
     
    public DHT(String symbol){
        this.symbol = symbol;
        this.circle = null;
        numberOfReplicas = 0;
        hashFunction = null;
    }
    
    public void setIndBuffer(int isize){
        INDEX_BUFFER_SIZE = isize;
    }
     
    public void setWriteBuffer(int capacity){
        sizelimit = capacity;
    }
     
/**
 * create the num of nodes with different combination of IP addr and port number     
 * @param num
 * @return nodes collection
 */
    public Collection<Part> createNodes(int num){
        String[] ipparts = IPseed.split("\\.");

        Random rand1 = new Random(Integer.valueOf(ipparts[0])*regx);
        Random rand2 = new Random(Integer.valueOf(ipparts[1])*regx);
        Random rand3 = new Random(Integer.valueOf(ipparts[2])*regx);
        Random rand4 = new Random(Integer.valueOf(ipparts[3])*regx);
        Random randp = new Random(portseed*regx);
         
        Collection<Part> nodelist = new ArrayList<Part>();
        for(int i=0;i<num;++i){
            Part node = new Part();
            int ip1 = rand1.nextInt(256);
            rand1.setSeed(ip1*(regx+i));
            int ip2 = rand2.nextInt(256);
            rand2.setSeed(ip2*(regx+i));
            int ip3 = rand3.nextInt(256);
            rand3.setSeed(ip3*(regx+i));
            int ip4 = rand4.nextInt(256);
            rand4.setSeed(ip4*(regx+i));
            int port = randp.nextInt(10000);
            randp.setSeed(port*regx);
            node.IP = ip1+"."+ip2+"."+ip3+"."+ip4;
            node.port = String.valueOf(port);
            node.stmtfile = "partition_"+i;//node.IP+"_"+port;

            nodelist.add(node);
        }
        return nodelist;
    }

/**
 *     create DHT network according to generated join-in nodes     
 */
    public void createDHT(Collection<Part> nodelist){
        for(Part node : nodelist){
            add(node);
        }
    }
     
/**
 * add node to DHT network 
 * @param node
 */
    public void add(Part node) {
        try{
            for (int i = 0; i < numberOfReplicas; i++) {
                circle.put(hashFunction.hash(node.toString()+":" + i), node);
            }
        }catch(NoSuchAlgorithmException e1){
            e1.printStackTrace();
        }catch(UnsupportedEncodingException e2){
            e2.printStackTrace();
        }
    }

/**
 * remove a node from the DHT network     
 * @param node
 */
    public void remove(Part node) {
        try{
            for (int i = 0; i < numberOfReplicas; i++) {
                circle.remove(hashFunction.hash(node.toString()+":" + i));
            }
        }catch(NoSuchAlgorithmException e1){
            e1.printStackTrace();
        }catch(UnsupportedEncodingException e2){
            e2.printStackTrace();
        }
    }

/**
 * position a node on DHT ring by an keyword string     
 * @param key
 * @return
 */
    public Part get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        try{
            BigInteger hash = hashFunction.hash(key);
            if (!circle.containsKey(hash)) {
                SortedMap<BigInteger, Part> tailMap = circle.tailMap(hash);
                hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return circle.get(hash);
        }catch(NoSuchAlgorithmException e1){
            e1.printStackTrace();
            return null;
        }catch(UnsupportedEncodingException e2){
            e2.printStackTrace();
            return null;
        }
    } 

    /**
     * assign a resource to a node
     * @param res
     * @return
     */
    public Part dht_assign(String res){
        Part node = (Part) get(res);
        if(node != null)
            return node;
        else 
            return null;
    }

/**
 * put a value to key position of node (here use sub as the key for storing statement)
 * @param res
 * @param node
 * @param value
 * @param ind_buffer
 * @param parts_stmt_info
 * @return 
 */
    public boolean updateIndex(String res, Part node, String[] value,
           Hashtable<String, Hashtable<Integer,Quad>> ind_buffer, int[] parts_stmt_info){
         
        int assign = Integer.parseInt(node.stmtfile.substring(node.stmtfile.indexOf("_")+1));
        String stmt;
        Hashtable<Integer,Quad> entry, oentry;
        Quad qd = new Quad();
        Quad oqd;
        int s_len, o_len;
        if(assign != -1){ 
            String sub = value[0];//value.substring(0,index);
            String obj = value[2];
            String pro = value[1];
            int res_types=0, res_typeo=0;
            int obj_types=0, obj_typeo=0;
            if(ind_buffer.containsKey(res)){
                entry = ind_buffer.get(res);
                if(entry.containsKey(assign)){
                    Quad tmp = entry.get(assign);
                    res_types = tmp.type_s;
                    res_typeo = tmp.type_o;
                }
            } else
                entry = new Hashtable<Integer, Quad>();
            // also save the semantic info in string
            if(sub.charAt(0) != '?'){ // obj is res
                // also should update res index in index buffer or my sql indectly
                s_len = pro.length()+obj.length();
                o_len = pro.length()+sub.length();
                // update sub index    
                if(res_types == 0){
                    res_types = 1;
                }
                 
                // update later after side     
                // update obj index
                oqd = new Quad();
                if(ind_buffer.containsKey(obj)){
                    oentry = ind_buffer.get(obj);
                    if(oentry.containsKey(assign)){
                        Quad tmp = ind_buffer.get(obj).get(assign);
                        obj_types = tmp.type_s;
                        obj_typeo = tmp.type_o;
                    }
                }else
                    oentry = new Hashtable<Integer, Quad>();
                
                if(obj_typeo == 0){
                    obj_typeo = 1;
                }
                oqd.type_s= obj_types;
                oqd.type_o = obj_typeo;
                oqd.o_bytes += o_len;
                oentry.put(assign, oqd);
                ind_buffer.put(obj, oentry);
            }else{ // obj is literal
                sub = sub.substring(1);
                s_len = pro.length()+obj.length();
                if(res_types == 0) res_types = 1;
            }
            // update sub index for both cases     
            qd.type_s = res_types;
            qd.type_o = res_typeo;
            qd.s_bytes += s_len;     
            entry.put(assign, qd);
            ind_buffer.put(res, entry);
            
            parts_stmt_info[assign]++;
            //use pending write     
            stmt = sub+" "+pro+" "+obj+" .";
               
            node.stmts.add(stmt);
            if(node.stmts.size() > sizelimit){
                RandomP.writeout(node.stmts,node.stmtfile,this.stmtfolder,this.symbol);
                node.stmts.clear(); 
            }
            // write out index     
            if(ind_buffer.size() >= INDEX_BUFFER_SIZE){
                mysql.createIndex_random(ind_buffer);
                ind_buffer.clear();
            }
            return true;
        }else return false;
    }
     
    class common {
        int nodeID;
        //statementID set for the certain resourses     
        ArrayList<String> stmts;
    }

/** 
 * print node statements into file with format as <node, stmtlist>
 * @param filename
 * @param nodelist
 */
    public void nodestmts_print(String filename, Part[] nodelist){
        File file = new File(filename);
           if(!file.exists()){
               try{
                   file.createNewFile();
               }catch(IOException e){
                   System.err.println("file create wrong!");
               }
           }
           try{
               BufferedWriter out = new BufferedWriter(new FileWriter(filename));
               
               for(int i=0;i<nodelist.length;++i){
                   Part node = nodelist[i];
                   out.write("node "+i+":"+node.toString()+"\n");
                   for(int j=0;j<node.stmts.size();++j){
                       out.write(node.stmts.get(j)+"\n");
                   }
               } 
               out.close();
           }catch(IOException e){
               System.out.println(e.getMessage());
           }
    }
     
    public void setParFolder(String folder){
        this.stmtfolder = folder;
    }
     
     /*public static BigInteger generateSHA1(String key, int keySize ) {
         try {
         MessageDigest md = MessageDigest.getInstance("SHA-1");
         byte[] hashMsg = md.digest(key.toString().getBytes());
         BigInteger bi = new BigInteger(hashMsg).abs();
         BigInteger mod = new BigInteger( Integer.toString( keySize ) );
         return bi.mod( mod );
         } catch (Exception e) {
         }
         return null;
         }*/
     
/*     public static void main(String[] args){
         HashFunction SHA1 = new HashFunction();
         int numberRep = 1;
         int numberNode = 100;
         String folder = "filebase";
         String stmtParFolder = "DHT_stmts";
    // create DHT network              
         DHT dht = new DHT(SHA1,numberRep,numberNode,stmtParFolder);
    // read stmts in for resource list     
         dht.pre_read(folder);
    // partition stmts     
         if(dht.storeall(folder)){
             System.out.println("statement size: "+dht.sum);
             HashMap<String,Integer> shareRes = Statistic.sumupCut(dht.circle, dht.resource_list);
             System.out.println("communication cost : "+Statistic.nodeCut(shareRes));
             Statistic.nodebalance(dht.circle);
         }
     }*/

}
