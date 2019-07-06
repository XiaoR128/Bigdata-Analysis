package sample;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducetwo extends Reducer<Text, Text, NullWritable, Text>{
	
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) 
			throws java.io.IOException ,InterruptedException {
		StringBuffer buf = new StringBuffer();
		Set<Integer> set = new HashSet<>();
		int n=6000;
		int max=600000;
		int min=1;
		
	    int[] result = new int[n];  
	    int count = 0;  
	    while(count < n) {  
	        int num = (int) (Math.random() * (max - min)) + min;  
	        boolean flag = true;  
	        for (int j = 0; j < n; j++) {  
	            if(num == result[j]){  
	                flag = false;  
	                break;  
	            }  
	        }  
	        if(flag){  
	            result[count] = num;  
	            count++;  
	        }  
	    }  
	    for(int k:result) {
	    	set.add(k);
	    }
	    int i=1;
	    
	    for(Text value:values) {
	    	if(set.contains(i)) {
	    		buf.append(value);
	    		buf.append("\n");
	    	}
	    	i++;
	    }
		String str = buf.toString().trim();
		
		context.write(NullWritable.get(), new Text(str));
	};
}
