package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Onemap extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String lines = value.toString();
		
		String[] line = lines.split("\\|");
		
		String str = line[10];
		context.write(new Text(str), new Text(lines));
	}
}
