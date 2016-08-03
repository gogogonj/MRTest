package com.bonc.test;

import com.bonc.input.newapi.DpiDecodeInputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DpiDecodeMain extends Configured implements Tool {
	
	public static void main(String[] arges) throws Exception {
		exec(arges);
	}

	public static void exec(String[] txt) throws Exception {
		ToolRunner.run(new Configuration(), new DpiDecodeMain(), txt);
	}

	public int run(String[] arg0) throws Exception {
		String input = arg0[0];
		String output = arg0[1];

		FileSystem dst = FileSystem.get(getConf());
		Path dstPath = new Path(output);
		dst.delete(dstPath, true);

		FileStatus[] fileStatus = dst.listStatus(new Path(input));

		Job job = new Job(getConf(), "decode");

		job.setJarByClass(DpiDecodeMain.class);
		job.setMapperClass(URLDecodeMapper.class);

		job.setInputFormatClass(DpiDecodeInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		for (int i = 0; i < fileStatus.length; i++) {
			FileInputFormat.addInputPath(job, fileStatus[i].getPath());
		}
		FileOutputFormat.setOutputPath(job, new Path(output));

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class URLDecodeMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String lines = value.toString();
			context.write(NullWritable.get(), new Text(lines));
		}
	}
}
