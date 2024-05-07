import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.io.IntWritable;

public class MapForwardCount extends Mapper<LongWritable, Text, Text, IntWritable>
{

	public void map(LongWritable offset, Text key, Context con) throws IOException, InterruptedException {

		String line = key.toString();

		String words[] = line.split("");

		for (String word : words) {

			Text outputKey = new Text(word);

			IntWritable outputValue = new IntWritable(1);

			con.write(outputKey.outputValue);

		}

	}

}
s