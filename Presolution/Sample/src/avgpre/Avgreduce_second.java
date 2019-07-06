package avgpre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Avgreduce_second extends Reducer<Text, Text, NullWritable, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer bf = new StringBuffer();
		List<String> list = new ArrayList<>();
		int flag = 1;
		double sum = 0;
		double count = 0;
		for(Text value : values) {
			String[] str = value.toString().split("\\|");
			String rate = str[6];
			if(rate.equals("?")) {
				flag=0;
				list.add(value.toString());
			}else {
				count++;
				sum+=Double.parseDouble(str[6]);
				bf.append(value.toString());
				bf.append("\n");
			}
		}
		if(flag==0) {
			String com = null;
			if(count==0) {
				com = "0.5";
			}else {
				com = String.valueOf(sum/count);
			}
			for(int i=0;i<list.size()-1;i++) {
				String s = list.get(i).replace("?", com);
				bf.append(s);
				bf.append("\n");
			}
			String s = list.get(list.size()-1).replace("?", com);
			bf.append(s);
		}
		context.write(NullWritable.get(), new Text(bf.toString().trim()));
	}
}
