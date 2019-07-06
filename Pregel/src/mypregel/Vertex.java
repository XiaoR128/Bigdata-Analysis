package mypregel;

import java.util.ArrayList;

public abstract class Vertex {
	
	private int id;
	protected ArrayList<Vertex> out_vertices=new ArrayList<>();
	int superstep=0;
	protected boolean active=true;
	protected ArrayList<Tuple> in_messege = new ArrayList<>();
	protected ArrayList<Tuple> out_messege = new ArrayList<>();
	
	/**
	 * 构造器
	 * @param id
	 */
	public Vertex(int id) {
		this.id = id;
	}
	
	
	/**
	 * 获取点的id号
	 * @return id
	 */
	public int getid() {
		return this.id;
	}
	
	/**
	 * compute抽象函数，等到具体类实现
	 */
	public abstract void compute();
	
	/**
	 * 从文本读取数据，构造点的出边
	 * @param v
	 */
	public void add_outvertices(Vertex v) {
		out_vertices.add(v);
	}
	
	/**
	 * 获取当前点的出边点
	 * @return
	 */
	public ArrayList<Vertex> get_outvertices(){
		return this.out_vertices;
	}
	
	/**
	 * 获取当前节点是否活跃
	 * @return
	 */
	public boolean get_active() {
		return this.active;
	}
	
	public void set_active(boolean act) {
		this.active = act;
	}
	
	/**
	 * 获取超级轮数
	 * @return
	 */
	public int getSuperstep() {
		return superstep;
	}
	
	public void setSuperstep(int newstep) {
		this.superstep=newstep;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof Vertex)) {
			return false;
		}
		if(obj == this){
			return true;
		}
		return this.id == ((Vertex)obj).getid();
	}
	
	@Override
	public int hashCode(){
        return this.id;
    }
	
	@Override
	public String toString() {
//		String s = ""+this.id+":";
//		for(Vertex a:this.out_vertices) {
//			s = s + a.id+" ";
//		}
		return String.valueOf(this.id);
	}
}
