package mypregel;

import java.util.ArrayList;

public class CountAggregator extends Aggregator{
	private int edgenum;
	
	@Override
	public String aggregate() {
		return "sum";
	}

	@Override
	public void report(ArrayList<Vertex> verticeslist) {
		String type = this.aggregate();
		int sum=0;
		if(type.equals("sum")) {
			for(Vertex v:verticeslist) {
				sum+=v.out_vertices.size();
			}
		}
		edgenum = sum;
	}
	
	public int getedgenum() {
		return this.edgenum;
	}
}
