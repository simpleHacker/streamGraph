package partitioning.graph.struct;

/**
 * resource infomation object
 * @author AI
 *
 */
public class Quad {
	public int type_s;
	public int type_o;
//	public float weight;
	public int weight; // only record the related edge number for this resource
	public int s_bytes;
	public int o_bytes;
	public Quad(){
		type_s = 0;
		type_o = 0;
		s_bytes = 0;
		o_bytes = 0;
		weight = 0;
	}
	
	@Override
	public String toString(){
		return "("+type_s+":"+type_o+":"+weight+":"+s_bytes+":"+o_bytes+")";
	}
	
/**************	
	@Override
	public Quad clone() {
		Quad obj2 = new Quad();
		obj2.type_o = this.type_o;
		obj2.type_s = this.type_s;
		obj2.o_bytes = this.o_bytes;
		obj2.s_bytes = this.s_bytes;
		obj2.weight = this.weight;
		return obj2;
	}
	
	public Quad update(Quad obj2){
		this.type_o |= obj2.type_o;
		this.type_s |= obj2.type_s;
		this.o_bytes += obj2.o_bytes;
		this.s_bytes += obj2.s_bytes;
		this.weight += obj2.weight;
		return this;
	}
	
	public boolean equal(Quad obj2){
		if(obj2.type_o != this.type_o)
			return false;
		if(obj2.type_s != this.type_s)
			return false;
		if(obj2.o_bytes != this.o_bytes)
			return false;
		if(obj2.s_bytes != this.s_bytes)
			return false;
		if(obj2.weight != this.weight)
			return false;
		return true;
	}
	***********************************/
}
