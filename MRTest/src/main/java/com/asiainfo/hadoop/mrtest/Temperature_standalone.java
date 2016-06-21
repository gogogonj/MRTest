package com.asiainfo.hadoop.mrtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 单机模式下运行 hadoop jar /home/wangwei/opt/test.jar com.asiainfo.hadoop.mrtest.Temperature_standalone /home/wangwei/opt/input /home/wangwei/opt/output
 * 
 * @author AI
 *
 */
public class Temperature_standalone {
	/**
	 * 
	 * 四个泛型类型分别代表：
	 * 
	 * KeyIn Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
	 * 
	 * ValueIn Mapper的输入数据的Value，这里是每行文字
	 * 
	 * KeyOut Mapper的输出数据的Key，这里是每行文字中的“年份”
	 * 
	 * ValueOut Mapper的输出数据的Value，这里是每行文字中的“气温”
	 */

	static class TempMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			System.out.println("Before Mapper: " + key + ", " + value);

			String line = value.toString();
			String year = line.substring(0, 4);
			int temperature = Integer.parseInt(line.substring(8));

			context.write(new Text(year), new IntWritable(temperature));

			System.out.println("After Mapper:" + new Text(year) + ", "
					+ new IntWritable(temperature));
		}
	}

	/**
	 * 
	 * 四个泛型类型分别代表：
	 * 
	 * KeyIn Reducer的输入数据的Key，这里是每行文字中的“年份”
	 * 
	 * ValueIn Reducer的输入数据的Value，这里是每行文字中的“气温”
	 * 
	 * KeyOut Reducer的输出数据的Key，这里是不重复的“年份”
	 * 
	 * ValueOut Reducer的输出数据的Value，这里是这一年中的“最高气温”
	 */

	static class TempReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int maxValue = Integer.MIN_VALUE;

			StringBuffer sb = new StringBuffer();
			// 取values的最大值
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
				sb.append(value).append(", ");
			}

			// 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
			System.out.println("Before Reduce: " + key + ", " + sb.toString());
			context.write(key, new IntWritable(maxValue));
			// 打印样本： After Reduce: 2000, 99
			System.out.println("After Reduce: " + key + ", " + maxValue);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Temperature_standalone");
		job.setJarByClass(Temperature_standalone.class);
		job.setMapperClass(TempMapper.class);
		job.setCombinerClass(TempReducer.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		conf.setStrings("mapred.reduce.tasks", "1");

		// 设置输入目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 设置输出目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 输出文件进行压缩
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		// 删除输出目录
		FileSystem filesys = FileSystem.get(conf);
		filesys.delete(new Path(args[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
