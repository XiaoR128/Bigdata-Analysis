package standard;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StandardMap extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.replace("\t","");
		String[] str = line.split("\\|");
		
		Pattern p1 = Pattern.compile(".+-.+-.+");
		Pattern p3 = Pattern.compile(".+\\s*.+,.+");
		
		Pattern p2 = Pattern.compile(".+¡æ");
		
		if(p1.matcher(str[4]).matches()) {
			String[] s = str[4].split("-");
			str[4] = s[0]+"/"+s[1]+"/"+s[2];
		}else if(p3.matcher(str[4]).matches()) {
			String[] s1 = str[4].split(" ");
			String day = s1[1].split(",")[0];
			String year = s1[1].split(",")[1];
			String month = tomonth(s1[0]);
			str[4] = year+"/"+month+"/"+day;
		}
		
		if(p1.matcher(str[8]).matches()) {
			String[] s = str[8].split("-");
			str[8] = s[0]+"/"+s[1]+"/"+s[2];
		}else if(p3.matcher(str[8]).matches()) {
			String[] s1 = str[8].split(" ");
			String day = s1[1].split(",")[0];
			String year = s1[1].split(",")[1];
			String month = tomonth(s1[0]);
			str[8] = year+"/"+month+"/"+day;
		}
		
		if(p2.matcher(str[5]).matches()) {
			String number = str[5].replace("¡æ", "");
			double n = Double.parseDouble(number);
			double f = n*1.8+32;
			String s = String.valueOf(f)+"¨H";
			str[5] = s;
		}
		
		double max=95.94;
		double min=59.03;
		if(!str[6].equals("?")) {
			double rating = Double.parseDouble(str[6]);
			double new_rate = (rating-min)/(max-min);
			str[6]=String.valueOf(new_rate);
		}
		
		
		StringBuffer bf = new StringBuffer();
		for(int i=0;i<str.length-1;i++) {
			bf.append(str[i]);
			bf.append("|");
		}
		bf.append(str[11]);
		
		context.write(new Text(str[10]), new Text(bf.toString()));
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
