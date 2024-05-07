public class WDriver {

public static void main(String[] args) {

// TODO Auto-generated method stub

	Configuration c = new Configuration();

	Job job= new Job(c, 'wordcount');

	job.setJarByclass(WDriver.class);

	job.setMapperclass(mapForwardcount.class);

	job.setReducerclass(ReducerForwardCount.class);

	job.setOutputKeyclass(Text class);

	job.setOutputValueclass(IntWritable class);

	FileInputFormat.addInputPath(job, new path(args[0]));

	FileOutputFormat.setOutputPath(job, new path(args[1]));

	System.exit(job waitForCompletion(true)?0:1);

}

}
