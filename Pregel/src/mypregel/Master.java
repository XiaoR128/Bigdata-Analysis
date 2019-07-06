package mypregel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import algorithm.PagerankVertex;
import algorithm.SSSPVertex;

public class Master {
	private ArrayList<Worker> workers=new ArrayList<>();
	private ArrayList<Vertex> vertices=new ArrayList<>();
	private int worknum;
	public Counter counter = new Counter();
	public CountAggregator aggregator = new CountAggregator();
	
	/**
	 * constructor
	 * @param workernum
	 * @param vertices
	 */
	public Master(int workernum,ArrayList<Vertex> vertices) {
		this.worknum = workernum;
		this.vertices = vertices;
		init_Counter();
	}
	
	/**
	 * another constructor
	 * @param workernum
	 * @throws IOException 
	 */
	public Master(int workernum,String str) throws IOException {
		this.worknum = workernum;
		load(str);
		init_Counter();
	}
	
	/**
	 * Master执行计算
	 */
	public void run() {
		while(active()) {
			superstep();
			messege();
		}
	}
	
	/**
	 * 带有combiner的重载
	 * @param combiner
	 */
	public void run(Combiner combiner) {
		while(active()) {
			superstep();
			messege(combiner);
		}
	}
	
	/**
	 * 将定点分区
	 * @return
	 */
	public Map<Integer, ArrayList<Vertex>> partition_vertices() {
		Map<Integer, ArrayList<Vertex>> map = new HashMap<>();
		for(Vertex v:this.vertices) {
			ArrayList<Vertex> vlist = new ArrayList<>();
			map.put(hashworker(v), vlist);
		}
		for(Vertex v:this.vertices) {
			map.get(hashworker(v)).add(v);
		}
		return map;
	}
	
	/**
	 * 求顶点的hash
	 * @param v
	 * @return
	 */
	public int hashworker(Vertex v) {
		return v.hashCode()%this.worknum;
	}
	
	/**
	 * 判断是否有活跃的顶点
	 * @return
	 */
	public boolean active() {
		for(Vertex v:vertices) {
			if(v.get_active()) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * 向各个worker添加节点
	 * @param partition
	 */
	public void add_worker(Map<Integer, ArrayList<Vertex>> partition) {
		for(ArrayList<Vertex> list:partition.values()) {
			Worker w = new Worker(list);
			workers.add(w);
//			w.run();
		}
	}
	
	/**
	 * 对于每一个worker执行程序
	 */
	public void superstep() {
		int count=0;
		Map<Integer, Long> map = new HashMap<>();
		for(Worker w:workers) {
			count+=1;
			long startTime =  System.currentTimeMillis();
			w.run();
			long endTime =  System.currentTimeMillis();
			map.put(count, endTime-startTime);
		}
		counter.set_worker_time(map);
	}
	
	/**
	 * 更新每一个顶点的接受信息
	 */
	public void messege() {
		for(Vertex v:vertices) {
			v.setSuperstep(v.getSuperstep()+1);
			v.in_messege.clear();
		}
		for(Worker w:workers) {
			w.messege();
		}
	}
	
	/**
	 * 重载
	 * @param com
	 */
	public void messege(Combiner com) {
		for(Vertex v:vertices) {
			v.setSuperstep(v.getSuperstep()+1);
			v.in_messege.clear();
		}
		for(Worker w:workers) {
			w.messege(com);
		}
	}
	
	/**
	 * 获取图中的点
	 * @return
	 */
	public ArrayList<Vertex> get_vertices(){
		return this.vertices;
	}
	
	public void init_Counter() {
		int i=0;
		Map<Integer, Integer> worker_vertices = new HashMap<>();
		Map<Integer, Integer> worker_edge = new HashMap<>();
		for(Worker w:workers) {
			i+=1;
			int num = w.getvertices().size();
			int sum = 0;
			for(Vertex v:w.getvertices()) {
				sum+=v.out_vertices.size();
			}
			worker_edge.put(i, sum);
			worker_vertices.put(i, num);
		}
		counter.set_worker_vertices(worker_vertices);
		counter.set__worker_edge(worker_edge);
	}
	
	public void print_counter() {
		System.out.println("*********************");
		System.out.println("各个worker顶点数:");
		for(int k:counter.get_worker_vertices().keySet()) {
			System.out.println("第"+k+"个worker顶点数:"+counter.get_worker_vertices().get(k));
		}
		System.out.println("*********************");
		System.out.println("各个worker边的条数:");
		for(int k:counter.get_worker_edge().keySet()) {
			System.out.println("第"+k+"个worker边的条数:"+counter.get_worker_edge().get(k));
		}
		System.out.println("*********************");
		System.out.println("各个worker边的时间:");
		for(int k:counter.get_worker_time().keySet()) {
			System.out.println("第"+k+"个worker执行一轮花费:"+counter.get_worker_time().get(k));
		}
	}
	
	public void Aggregator_edgecount() {
		aggregator.report(vertices);
		int edgenum = aggregator.getedgenum();
		System.out.println("边的数量:"+edgenum);
	}
	
	/**
	 * 将划分的图保存到文件里面
	 */
	public void save() {
		Map<Integer, ArrayList<Vertex>> partition = partition_vertices();
		add_worker(partition);
		int count=0;
		for(Worker w:workers) {
			StringBuffer strb = new StringBuffer();
			count+=1;
			for(Vertex v:w.getvertices()) {
				strb.append(v.getid()+" ");
				for(Vertex vv:v.out_vertices) {
					strb.append(vv.getid()+" ");
				}
				strb.append("\n");
			}
			write_disk(strb.toString().trim(),count);
		}
	}
	
	/**
	 * 写回磁盘
	 * @param strbuf
	 */
	public void write_disk(String strbuf,int count){
		FileOutputStream output;
		try {
			output = new FileOutputStream("Partition_Save/"+"Save"+count+".txt");
			output.write(strbuf.getBytes());
			output.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 加载图数据
	 * @param str
	 * @throws IOException
	 */
	public void load(String str) throws IOException {
		if(str.equals("PageRank")) {
			load_page();
		}else if(str.equals("SSSP")) {
			load_sssp();
		}
	}
	
	/**
	 * 加载Pagerank点
	 * @throws IOException
	 */
	public void load_page() throws IOException {
//		int count = 1;
		Map<String, Vertex> map = new TreeMap<>();
		
		Set<String> set = new HashSet<>();
		for(int count=1;count<=worknum;count++) {
			ArrayList<Vertex> verli = new ArrayList<>();
			
			FileInputStream inputStream = null;
			Scanner sc = null;
			try {
			    inputStream = new FileInputStream("Partition_Save/"+"Save"+count+".txt");
			    sc = new Scanner(inputStream, "UTF-8");
			    while (sc.hasNextLine()) {
			        String[] line = sc.nextLine().split(" ");
			        Vertex vv = null;
			        if(!set.contains(line[0])) {
			        	set.add(line[0]);
			        	vv = new PagerankVertex(Integer.parseInt(line[0]),1.0/875713);
			        	map.put(line[0], vv);
			        }else {
			        	vv = map.get(line[0]);
			        }
			        for(int i=1;i<line.length;i++) {
				        String v = line[i];
				        Vertex nv = null;
				        
				        if(!set.contains(v)) {
				        	set.add(v);
				        	nv = new PagerankVertex(Integer.parseInt(line[i]),1.0/875713);
				        	vv.add_outvertices(nv);
				        	map.put(line[i], nv);
				        }else {
				        	nv = map.get(line[i]);
				        	vv.add_outvertices(nv);
				        }
			        }
			        verli.add(vv);
			    }
			    Worker worker = new Worker(verli);
			    workers.add(worker);
			    if (sc.ioException() != null) {
			        throw sc.ioException();
			    }
			} finally {
			    if (inputStream != null) {
			        inputStream.close();
			    }
			    if (sc != null) {
			        sc.close();
			    }
			}
			
		}
		for(Vertex v:map.values()) {
			vertices.add(v);
		}
	}
	
	/**
	 * 加载SSSP点
	 * @throws IOException
	 */
	public void load_sssp() throws IOException{
		int number=0;
		
		Map<String, Vertex> map = new TreeMap<>();
		
		Set<String> set = new HashSet<>();
		for(int count=1;count<=worknum;count++) {
			ArrayList<Vertex> verli = new ArrayList<>();
			
			FileInputStream inputStream = null;
			Scanner sc = null;
			try {
			    inputStream = new FileInputStream("Partition_Save/"+"Save"+count+".txt");
			    sc = new Scanner(inputStream, "UTF-8");
			    while (sc.hasNextLine()) {
			        String[] line = sc.nextLine().split(" ");
			        Vertex vv = null;
			        if(!set.contains(line[0])) {
			        	set.add(line[0]);
			        	vv = new SSSPVertex(Integer.parseInt(line[0]),Double.MAX_VALUE);
			        	if(number==0) {
			        		number+=1;
			        		((SSSPVertex)vv).setvalue(0.0);
			        	}
			        	map.put(line[0], vv);
			        }else {
			        	vv = map.get(line[0]);
			        }
			        for(int i=1;i<line.length;i++) {
				        String v = line[i];
				        Vertex nv = null;
				        
				        if(!set.contains(v)) {
				        	set.add(v);
				        	nv = new SSSPVertex(Integer.parseInt(line[i]),Double.MAX_VALUE);
				        	vv.add_outvertices(nv);
				        	map.put(line[i], nv);
				        }else {
				        	nv = map.get(line[i]);
				        	vv.add_outvertices(nv);
				        }
			        }
			        verli.add(vv);
			    }
			    Worker worker = new Worker(verli);
			    workers.add(worker);
			    if (sc.ioException() != null) {
			        throw sc.ioException();
			    }
			} finally {
			    if (inputStream != null) {
			        inputStream.close();
			    }
			    if (sc != null) {
			        sc.close();
			    }
			}
			
		}
		for(Vertex v:map.values()) {
			vertices.add(v);
		}
	}
}
