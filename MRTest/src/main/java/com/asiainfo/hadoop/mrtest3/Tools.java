package com.asiainfo.hadoop.mrtest3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

class Tools extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		String inputpath = arg0[0];
		String outputpath = arg0[1];

		Configuration conf = getConf();
		// 遍历目录及子目录
		conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true");

		Job job = Job.getInstance(conf, "test3");
		job.setJarByClass(Tools.class);
		job.setMapperClass(DpiLabelMapper.class);
		job.setReducerClass(DpiLabelReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputpath));

		FileOutputFormat.setOutputPath(job, new Path(outputpath));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

		MultipleOutputs.addNamedOutput(job, "NullKey", TextOutputFormat.class,
				NullWritable.class, Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

		FileSystem filesys = FileSystem.get(conf);
		filesys.delete(new Path(outputpath), true);

		return job.waitForCompletion(true) ? 0 : 1;

	}

}
