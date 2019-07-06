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
	 * ������
	 * @param id
	 */
	public Vertex(int id) {
		this.id = id;
	}
	
	
	/**
	 * ��ȡ���id��
	 * @return id
	 */
	public int getid() {
		return this.id;
	}
	
	/**
	 * compute���������ȵ�������ʵ��
	 */
	public abstract void compute();
	
	/**
	 * ���ı���ȡ���ݣ������ĳ���
	 * @param v
	 */
	public void add_outvertices(Vertex v) {
		out_vertices.add(v);
	}
	
	/**
	 * ��ȡ��ǰ��ĳ��ߵ�
	 * @return
	 */
	public ArrayList<Vertex> get_outvertices(){
		return this.out_vertices;
	}
	
	/**
	 * ��ȡ��ǰ�ڵ��Ƿ��Ծ
	 * @return
	 */
	public boolean get_active() {
		return this.active;
	}
	
	public void set_active(boolean act) {
		this.active = act;
	}
	
	/**
	 * ��ȡ��������
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
