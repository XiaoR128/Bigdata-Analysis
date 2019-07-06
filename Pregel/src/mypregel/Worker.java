package mypregel;

import java.util.ArrayList;

public class Worker {
	private ArrayList<Vertex> vertices;
	public Worker(ArrayList<Vertex> vertices) {
		this.vertices = vertices;
	}
	
	/**
	 * ��ÿһ����Ծ����ִ��compute����
	 */
	public void run() {
		for(Vertex v:vertices) {
			if(v.get_active()) {
				v.compute();
			}
		}
	}
	
	public ArrayList<Vertex> getvertices(){
		return this.vertices;
	}
	
	/**
	 * ����ÿһ������Ľ�����Ϣ
	 */
	public void messege() {
		for(Vertex v:vertices) {
			for(Tuple tuple : v.out_messege) {
				Tuple tt = new Tuple(v,tuple.value);
				Vertex vv = tuple.vertex;
				vv.in_messege.add(tt);
//				vv.set_active(true);
			}
		}
	}
	
	/**
	 * ʹ��combiner�����ĸ��¶�����Ϣ�ķ���
	 * @param com
	 */
	public void messege(Combiner com) {
		com.combine(vertices);
	}
}
