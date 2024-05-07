import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class MapLog extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text ipAddress = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
 	    String[] parts = line.split(" - - ");
 	    if (parts.length >= 1) {
 	    	String ipAddressStr = parts[0].trim();
 	    	ipAddress.set(ipAddressStr);
 	    	context.write(ipAddress, new IntWritable(1));
 	    }
 	}
}
