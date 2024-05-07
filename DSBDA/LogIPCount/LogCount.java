import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class LogCount {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
Configuration conf = new Configuration();
		
		Job job = new Job(conf,"logfile");
	    
	   
		job.setJarByClass(LogCount.class);
	    job.setMapperClass(MapLog.class);
	    job.setReducerClass(ReduceLog.class);
	 	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	 	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 	
	    System.exit(job.waitForCompletion(true)?0:1);


	}

}