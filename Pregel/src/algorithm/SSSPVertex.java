package algorithm;

import mypregel.Tuple;
import mypregel.Vertex;

public class SSSPVertex extends Vertex{
	private double value;
	private Vertex prevertex;
	
	/**
	 * Constructor
	 * @param id
	 * @param value
	 */
	public SSSPVertex(int id,double value) {
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
	
	/**
	 * 返回前一个点
	 * @return
	 */
	public Vertex get_prevertex() {
		return this.prevertex;
	}
	
	@Override
	public void compute() {
		if(getSuperstep()<50) {
			double minst = Double.MAX_VALUE;
			for(Tuple t:this.in_messege) {
				if(t.value+1<minst) {
					minst=t.value+1;
					this.prevertex = t.vertex;
				}
			}
			if(this.value<minst) {
				minst = this.value;
			}
			this.value = minst;
			this.out_messege.clear();
			for(Vertex v:this.out_vertices) {
				this.out_messege.add(new Tuple(v, minst));
			}
		}else {
			this.active=false;
		}
	}
}
