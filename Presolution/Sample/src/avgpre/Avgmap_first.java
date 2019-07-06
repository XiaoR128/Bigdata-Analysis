package avgpre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Avgmap_first extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace("\t", "");
		String[] str = line.split("\\|");
		
		String s = str[9]+str[10];
		
		context.write(new Text(s), new Text(line));
		
	}
	
}
