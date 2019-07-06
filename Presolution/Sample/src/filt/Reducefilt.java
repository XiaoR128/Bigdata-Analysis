package filt;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducefilt extends Reducer<Text, Text, NullWritable, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer bf = new StringBuffer();
		for(Text value:values) {
			bf.append(value);
			bf.append("\n");
		}
		String str = bf.toString().trim();
		context.write(NullWritable.get(), new Text(str));
	}
}
