package avgpre;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Avgmap_second extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] str = line.split("\\|");
		
		DecimalFormat df1 = new DecimalFormat("#.000");
		DecimalFormat df2 = new DecimalFormat("#.00");
		DecimalFormat df3 = new DecimalFormat("#.0");
		
		double longitude = Double.parseDouble(str[1]);
		double latitude = Double.parseDouble(str[2]);
		double altitude = Double.parseDouble(str[3]);
		
		String lo = df1.format(longitude);
		String la = df2.format(latitude);
		String al = df3.format(altitude);
		
		String lol = lo.substring(0, getIndex(lo , '.')+3);
		String lal = la.substring(0, getIndex(la , '.')+2);
		String all = al.substring(0, getIndex(al , '.'));
		String s = lol+lal+all;
		
		context.write(new Text(s), new Text(line));
	}
	
	public static int getIndex(String str , char ch) {
		for (int i = 0 ; i < str.length() ; i ++) {
		if(str.charAt(i) == (char)'.') {
		     return i;
		}
		}
		return -1;
    }
}
