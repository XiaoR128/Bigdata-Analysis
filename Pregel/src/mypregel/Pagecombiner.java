package mypregel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Pagecombiner extends Combiner{

	@Override
	public void combine(ArrayList<Vertex> vertices) {
		Map<Vertex, Double> map = new HashMap<>();
		for(Vertex v:vertices) {
			for(Tuple tuple : v.out_messege) {
				if(!map.containsKey(tuple.vertex)) {
					map.put(tuple.vertex, tuple.value);
				}else {
					double val = map.get(tuple.vertex);
					double newval = val+tuple.value;
					map.remove(tuple.vertex);
					map.put(tuple.vertex, newval);
				}
			}
		}
		for(Vertex v:map.keySet()) {
			Tuple e = new Tuple(v, map.get(v));
			v.in_messege.add(e);
		}
	}
	
}
