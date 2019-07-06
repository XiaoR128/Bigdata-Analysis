package mypregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SSSPcombiner extends Combiner{

	@Override
	public void combine(ArrayList<Vertex> vertices) {
		Map<Vertex, Double> map = new HashMap<>();
		Map<Vertex, Vertex> vertex_map = new HashMap<>();
		
		for(Vertex v:vertices) {
			for(Tuple tuple : v.out_messege) {
				if(!map.containsKey(tuple.vertex)) {
					map.put(tuple.vertex, tuple.value);
					vertex_map.put(tuple.vertex,v);
				}else {
					double val = map.get(tuple.vertex);
					if(tuple.value<val) {
						map.remove(tuple.vertex);
						map.put(tuple.vertex, tuple.value);
						vertex_map.remove(tuple.vertex);
						vertex_map.put(tuple.vertex, v);
					}
				}
			}
		}
		for(Vertex v:map.keySet()) {
			Tuple e = new Tuple(vertex_map.get(v), map.get(v));
			v.in_messege.add(e);
		}
	}
	
}
