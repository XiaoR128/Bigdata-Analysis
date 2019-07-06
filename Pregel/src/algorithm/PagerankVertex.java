package algorithm;

import java.util.ArrayList;

import mypregel.Tuple;
import mypregel.Vertex;

public class PagerankVertex extends Vertex{
	private double value;
	private final int vertices_num = 875713;
	
	/**
	 * Constructor
	 * @param id
	 * @param value
	 */
	public PagerankVertex(int id,double value) {
		super(id);
		this.value = value;
	}
	
	/**
	 * 设置点的值
	 * @param value
	 */
	public void setvalue(double value) {
		this.value = value;
	}
	
	/**
	 * 获取点的值
	 * @return
	 */
	public double getvalue() {
		return this.value;
	}
	
	
	@Override
	public void compute() {
		
		if(getSuperstep()<50) {
			double val = (0.15/vertices_num)+0.85*sum(in_messege);
			this.setvalue(val);
			double out_val = val/this.out_vertices.size();
			this.out_messege.clear();
			for(Vertex v:this.out_vertices) {
				this.out_messege.add(new Tuple(v, out_val));
			}
		}else {
			this.active=false;
		}
	}
	
	public double sum(ArrayList<Tuple> in_messege) {
		double sum=0.0;
		if(in_messege.size()!=0) {
			for(Tuple t:in_messege) {
				sum=sum+t.value;
			}
		}
		return sum;
	}
	
}
