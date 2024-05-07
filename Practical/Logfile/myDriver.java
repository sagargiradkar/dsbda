import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class myDriver {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		Configuration c = new Configuration();
		Job job = new Job(c, "WordCount");
		job.setJarByClass(myDriver.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		System.exit(job.waitForCompletion(true)?0:1);
		job.waitForCompletion(true);
		
		FileSystem fs= FileSystem.get(URI.create("hdfs://localhost:9000"+args[1]),c);
	    String Fname=fs.listStatus(new Path(args[1]))[1].getPath().toString();
	    							
	    FSDataInputStream in=null;
	    							
    	try
	    {
	    in=fs.open(new Path(Fname));
	    
	    String maxIp=null;
	    int maxCount=-1,intCount=-1;
	    int i=0;
	    
	    String line=null;
	    
	    while ((line=in.readLine())!=null)
	    {
	    	String words[]=line.split("\t");
	    	String ip =words[0];
	    	String cnt=words[1];
	    	
	    	intCount=Integer.parseInt(cnt);
	    	if(i == 0)
	    	{
	    		maxCount=intCount;
	    		maxIp=ip;
	    		i++;
	    	}
	    	else {
	    		if(intCount > maxCount) {
	    			maxCount = intCount;
	    			maxIp = ip;
	    		}
	    	}
	    }
	    
	    System.out.println("Max No. of logged in ip="+maxIp+" with count: "+maxCount);
	    in.close();
	    }
    	catch(IOException except)
    	{
    		except.printStackTrace();
    	}
	}

}
