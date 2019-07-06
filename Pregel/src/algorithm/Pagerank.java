package algorithm;

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

import mypregel.Master;
import mypregel.Pagecombiner;
import mypregel.Vertex;

public class Pagerank {
	public static void main(String[] args) throws IOException {
//		ArrayList<Vertex> vertices = Pagerank.get_vertices();
//		Master master = new Master(10, vertices);
//		master.save();
		Master master = new Master(10, "PageRank");
		master.run(new Pagecombiner());
//		master.run();
		ArrayList<Vertex> new_vertices = master.get_vertices();
		StringBuffer strbuf = new StringBuffer();
		for(Vertex v:new_vertices) {
			double value = ((PagerankVertex)v).getvalue();
			int id = ((PagerankVertex)v).getid();
			strbuf.append("ID:"+id+"    value:"+value+"\n");
		}
		Pagerank parank = new Pagerank();
		parank.write_disk(strbuf);
		master.print_counter();
	}
	
	/**
	 * 将结果写回磁盘
	 * @param strbuf
	 */
	public void write_disk(StringBuffer strbuf){
		FileOutputStream output;
		try {
			output = new FileOutputStream("Pagerank.txt");
			output.write(strbuf.toString().getBytes());
			output.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/**
	 * 从文件中读取出点的集合
	 * @return
	 * @throws IOException
	 */
	public static ArrayList<Vertex> get_vertices() throws IOException{
		Set<String> set = new HashSet<>();
		TreeMap<Integer, Vertex> map = new TreeMap<>();
		ArrayList<Vertex> vertices_mid = new ArrayList<>();
		
		FileInputStream inputStream = null;
		Scanner sc = null;
		try {
		    inputStream = new FileInputStream("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab3\\web-Google.txt");
		    sc = new Scanner(inputStream, "UTF-8");
		    while (sc.hasNextLine()) {
		        String[] line = sc.nextLine().split("	");
//		    	String[] line = sc.nextLine().split("k");
		        String v = line[0];
		        if(!set.contains(v)) {
		        	set.add(v);
		        	map.put(Integer.parseInt(line[0]), new PagerankVertex(Integer.parseInt(v),1.0/875713));
		        }
		    }
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
		
//		set.clear();
		FileInputStream inputStream2 = null;
		Scanner sc2 = null;
		Set<String> set2 = new HashSet<>();
		Map<Integer, Vertex> map2 = new HashMap<>();
		try {
		    inputStream2 = new FileInputStream("C:\\Users\\11051\\Desktop\\大数据分析\\Lab\\Lab3\\web-Google.txt");
		    sc2 = new Scanner(inputStream2, "UTF-8");
		    while (sc2.hasNextLine()) {
		        String[] line = sc2.nextLine().split("	");
//		    	String[] line = sc2.nextLine().split("k");
		        String v = line[0];
		        String vto = line[1];
		        if(!set.contains(vto)) {
		        	if(!set2.contains(vto)) {
			        	Vertex e = new PagerankVertex(Integer.parseInt(vto),1.0/875713);
			        	map.get(Integer.parseInt(v)).add_outvertices(e);
			        	map2.put(Integer.parseInt(vto), e);
			        	vertices_mid.add(e);
			        	set2.add(vto);
		        	}else {
		        		map.get(Integer.parseInt(v)).add_outvertices(map2.get(Integer.parseInt(vto)));
		        	}
		        }else {
		        	map.get(Integer.parseInt(v)).add_outvertices(map.get(Integer.parseInt(vto)));
		        }
		    }
		    if (sc2.ioException() != null) {
		        throw sc2.ioException();
		    }
		} finally {
		    if (inputStream2 != null) {
		        inputStream2.close();
		    }
		    if (sc2 != null) {
		        sc2.close();
		    }
		}
		set2.clear();
		map2.clear();
		set.clear();
		ArrayList<Vertex> vertices = new ArrayList<>();
		for(Vertex v:map.values()) {
			vertices.add(v);
		}
		vertices.addAll(vertices_mid);
		map.clear();
		return vertices;
	}
}
