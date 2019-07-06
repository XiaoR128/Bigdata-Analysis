package mapreduce;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Onereduce extends Reducer<Text, Text, NullWritable, Text>{
	protected void reduce(Text key, java.lang.Iterable<Text> values, Context context) 
			throws java.io.IOException ,InterruptedException {
		/*文本抽样*/
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
		
		/*文本过滤*/
		List<String> li = new ArrayList<>();
		String[] newstr = str.split("\n");
		for(String line:newstr) {
			String[] s1 = line.split("\\|");
			
			double longitude = Double.parseDouble(s1[1]); //[8.1461259, 11.1993265]
			double latitude = Double.parseDouble(s1[2]);  //[56.5824856, 57.750511]
			String rating = s1[6];
			
			if(rating.equals("?")) {
				li.add(line);
			}else {
				double rate = Double.parseDouble(rating);
				if((longitude<=11.1993265&&longitude>=8.1461259) && (latitude>=56.5824856&&latitude<=57.750511)
						&& (rate>=59.5&&rate<=95.92)) {
					li.add(line);
				}
			}
		}
		
		StringBuffer bb = new StringBuffer();
		/*数据归一化和标准化*/
		for(String newline : li) {
			String[] st = newline.split("\\|");
			
			Pattern p1 = Pattern.compile(".+-.+-.+");
			Pattern p3 = Pattern.compile(".+\\s*.+,.+");
			
			Pattern p2 = Pattern.compile(".+℃");
			
			if(p1.matcher(st[4]).matches()) {
				String[] s = st[4].split("-");
				st[4] = s[0]+"/"+s[1]+"/"+s[2];
			}else if(p3.matcher(st[4]).matches()) {
				String[] s1 = st[4].split(" ");
				String day = s1[1].split(",")[0];
				String year = s1[1].split(",")[1];
				String month = tomonth(s1[0]);
				st[4] = year+"/"+month+"/"+day;
			}
			
			if(p1.matcher(st[8]).matches()) {
				String[] s = st[8].split("-");
				st[8] = s[0]+"/"+s[1]+"/"+s[2];
			}else if(p3.matcher(st[8]).matches()) {
				String[] s1 = st[8].split(" ");
				String day = s1[1].split(",")[0];
				String year = s1[1].split(",")[1];
				String month = tomonth(s1[0]);
				st[8] = year+"/"+month+"/"+day;
			}
			
			if(p2.matcher(st[5]).matches()) {
				String number = st[5].replace("℃", "");
				double nn = Double.parseDouble(number);
				double f = nn*1.8+32;
				String s = String.valueOf(f)+"℉";
				st[5] = s;
			}
			
			double maxx=95.94;
			double minn=59.03;
			if(!st[6].equals("?")) {
				double rating = Double.parseDouble(st[6]);
				double new_rate = (rating-minn)/(maxx-minn);
				st[6]=String.valueOf(new_rate);
			}
			
			StringBuffer bff = new StringBuffer();
			for(int j=0;j<st.length-1;j++) {
				bff.append(st[j]);
				bff.append("|");
			}
			bff.append(st[11]);
			bb.append(bff.toString());
			bb.append("\n");
		}
		String strin = bb.toString().trim();
		context.write(NullWritable.get(), new Text(strin));
	}
	
	public String tomonth(String m) {
		String s = null;
		switch (m) {
		case "January":
			s="01";
			break;
		case "February":
			s="02";
			break;
		case "March":
			s="03";
			break;
		case "April":
			s="04";
			break;
		case "May":
			s="05";
			break;
		case "June":
			s="06";
			break;
		case "July":
			s="07";
			break;
		case "August":
			s="08";
			break;
		case "September":
			s="09";
			break;
		case "October":
			s="10";
			break;
		case "November":
			s="11";
			break;
		case "December":
			s="12";
			break;
		default:
			break;
		}
		return s;
	}
}
