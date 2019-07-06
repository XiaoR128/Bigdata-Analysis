package mypregel;

import java.util.ArrayList;

public class Worker {
	private ArrayList<Vertex> vertices;
	public Worker(ArrayList<Vertex> vertices) {
		this.vertices = vertices;
	}
	
	/**
	 * 在每一个活跃点上执行compute函数
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
	 * 更新每一个顶点的接受信息
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
	 * 使用combiner方法的更新顶点信息的方法
	 * @param com
	 */
	public void messege(Combiner com) {
		com.combine(vertices);
	}
}
