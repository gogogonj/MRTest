package com.asiainfo.hadoop.mrtest;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 在伪分布式环境下执行：
 * hadoop jar /home/wangwei/opt/test.jar com.asiainfo.hadoop.mrtest.Temperature4 /user/wangwei/test/ /user/wangwei/test/
 * 
 * @author AI
 *
 */
public class Temperature4 extends Configured implements Tool {
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

			// 打印样本: Before Mapper: 0, 2000010115
			System.out.print("Before Mapper: " + key + ", " + value);

			String line = value.toString();
			String year = line.substring(0, 4);
			int temperature = Integer.parseInt(line.substring(8));

			context.write(new Text(year), new IntWritable(temperature));

			// 打印样本: After Mapper:2000, 15
			System.out.println("======" + "After Mapper:" + new Text(year)
					+ ", " + new IntWritable(temperature));
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
			int sum = 0;
			// 取values的和
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJarByClass(Temperature4.class);
		job.setMapperClass(TempMapper.class);
		job.setCombinerClass(TempReducer.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 设置输入目录
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 设置输出目录
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 输出文件进行压缩
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		// 删除输出目录
		FileSystem filesys = FileSystem.get(getConf());
		filesys.delete(new Path(args[1]), true);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Temperature4(), args);
	}

}
