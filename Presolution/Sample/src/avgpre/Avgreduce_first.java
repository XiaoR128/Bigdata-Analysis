package avgpre;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Avgreduce_first extends Reducer<Text, Text, NullWritable, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer bf = new StringBuffer();
		List<String> list = new ArrayList<>();
		int flag = 1;
		int sum = 0;
		int count = 0;
		for(Text value : values) {
			String[] str = value.toString().split("\\|");
			String income = str[11];
			if(income.equals("?")) {
				flag=0;
				list.add(value.toString());
			}else {
				count++;
				sum+=Integer.parseInt(str[11]);
				bf.append(value.toString());
				bf.append("\n");
			}
		}
		if(flag==0) {
			String com = String.valueOf(sum/count);
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
