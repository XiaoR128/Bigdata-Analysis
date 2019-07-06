package mypregel;

import java.util.HashMap;
import java.util.Map;

public class Counter {
	private Map<Integer, Integer> worker_vertices;
	private Map<Integer, Integer> worker_edge;
	private Map<Integer, Long> worker_time = new HashMap<>();
	
	/**
	 * 
	 * @return
	 */
	public Map<Integer, Integer> get_worker_vertices() {
		return this.worker_vertices;
	}
	
	/**
	 * 
	 * @return
	 */
	public Map<Integer, Integer> get_worker_edge(){
		return this.worker_edge;
	}
	
	/**
	 * 
	 * @return
	 */
	public Map<Integer, Long> get_worker_time(){
		return this.worker_time;
	}
	
	public void set_worker_vertices(Map<Integer, Integer> worker_vertices) {
		this.worker_vertices = worker_vertices;
	}
	
	public void set__worker_edge(Map<Integer, Integer> worker_edge) {
		this.worker_edge = worker_edge;
	}
	
	public void set_worker_time(Map<Integer, Long> worker_time) {
		this.worker_time = worker_time;
	}
	
}
