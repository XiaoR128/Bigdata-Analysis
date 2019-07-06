package algorithm;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import mypregel.Master;
import mypregel.SSSPcombiner;
import mypregel.Vertex;

public class SSSP {
	public static void main(String[] args) throws IOException {
//		SSSP sssp = new SSSP();
		Master master = new Master(10, "SSSP");
		master.run(new SSSPcombiner());
		ArrayList<Vertex> new_vertices = master.get_vertices();
		
		FileWriter filew = new FileWriter("SSSP.txt");
		for(Vertex v:new_vertices) {
			StringBuffer strbuf = new StringBuffer();
			SSSPVertex spvertex = (SSSPVertex)v;
			double value = spvertex.getvalue();
			int id = spvertex.getid();
//			strbuf = id+":"+value+"\t"+id;
			strbuf.append("ID:"+id+"    Value:"+value);
			if(value!=Double.MAX_VALUE && value!=0.0) {
				Vertex p = spvertex.get_prevertex();
				strbuf.append("     "+id);
				while(p.getid()!=0) {
//					strbuf = strbuf+"<-"+p.getid();
					strbuf.append("<-"+p.getid());
					p = ((SSSPVertex)p).get_prevertex();
				}
				strbuf.append("<-"+0);
			}
			strbuf.append("\n");
			filew.write(strbuf.toString());
		}
		filew.close();
		
		master.print_counter();
	}
}
