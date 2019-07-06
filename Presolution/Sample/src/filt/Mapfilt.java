package filt;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapfilt extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replaceAll("\t", "");
		String[] str = line.split("\\|");
		
		double longitude = Double.parseDouble(str[1]); //[8.1461259, 11.1993265]
		double latitude = Double.parseDouble(str[2]);  //[56.5824856, 57.750511]
		String rating = str[6];
		
		if(rating.equals("?")) {
			context.write(new Text(str[10]), new Text(line));
		}else {
			double rate = Double.parseDouble(rating);
			if((longitude<=11.1993265&&longitude>=8.1461259) && (latitude>=56.5824856&&latitude<=57.750511)
					&& (rate>=59.5&&rate<=95.92)) {
				context.write(new Text(str[10]), new Text(line));
			}
		}
		
	}
}
