package com.asiainfo.hadoop.ChainMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// hadoop jar /home/wangwei/opt/mr.jar com.asiainfo.hadoop.ChainMapper.Drive
public class Drive {

	public static void main(String[] args) throws Exception {
		
		String config = "aaa.xml";
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "JobName");
		job.setJarByClass(Drive.class);
		
		conf.set("sasa", "100");
		conf.addResource(config);
		System.out.println("license========"+conf.get("license"));
		System.out.println("sasa========"+conf.get("sasa"));

		Configuration map1Conf = new Configuration(false);
		ChainMapper.addMapper(job, MapClass1.class, LongWritable.class,
				Text.class, Text.class, Text.class, map1Conf);

		Configuration map2Conf = new Configuration(false);
		ChainMapper.addMapper(job, MapClass2.class, Text.class, Text.class,
				Text.class, Text.class, map2Conf);

		job.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(
				"/home/wangwei/opt/input/chain"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/home/wangwei/opt/output/chain"));

		if (!job.waitForCompletion(true))
			return;
	}

}
