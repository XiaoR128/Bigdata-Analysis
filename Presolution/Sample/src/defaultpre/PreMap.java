package defaultpre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreMap extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace("\t", "");
		String[] str = line.split("\\|");
		if(str[6].equals("?")) {
			str[6] = new String("0.42");
		}
		if(str[11].equals("?")) {
			str[11] = new String("3500");
		}
		StringBuffer bf = new StringBuffer();
		for(int i =0;i<str.length-1;i++) {
			bf.append(str[i]);
			bf.append("|");
		}
		bf.append(str[11]);
		context.write(new Text(str[10]), new Text(bf.toString()));
	}
}
