package com.asiainfo.hadoop.chain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//hadoop jar /home/wangwei/opt/mr.jar com.asiainfo.hadoop.chain.ChainDriver2 -i /home/wangwei/opt/input/chain2 -o /home/wangwei/opt/output/chain2 -delimiter , -reducer 2
public class ChainDriver2 extends Configured implements Tool {

	/**
	 * ChainReducer 实战 验证多个reducer的整合 逻辑：寻找最大值
	 * 
	 * @param args
	 */

	private String input = "/home/wangwei/opt/input/chain2";
	private String output = "/home/wangwei/opt/output/chain2";
	private String delimiter = ",";
	private int reducer = 2;

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ChainDriver2(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		
//		configureArgs(arg0);
		
//		checkArgs();
		
		Configuration conf = getConf();
		conf.set("delimiter", delimiter);
		
		JobConf job = new JobConf(conf, ChainDriver2.class);

		ChainMapper.addMapper(job, MaxMapper.class, LongWritable.class,
				Text.class, Text.class, IntWritable.class, true, new JobConf(
						false));

		ChainMapper.addMapper(job, MergeMaxMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, true,
				new JobConf(false));

		ChainReducer.setReducer(job, MaxReducer.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, true,
				new JobConf(false));
		
		ChainReducer.addMapper(job, MergeMaxMapper.class, Text.class,
				IntWritable.class, Text.class, IntWritable.class, false,
				new JobConf(false));
		
		job.setJarByClass(ChainDriver2.class);
		job.setJobName("ChainReducer test job");

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * job.setMapperClass(MaxMapper.class);
		 * job.setReducerClass(MaxReducer.class);
		 */
		job.setInputFormat(TextInputFormat.class);
		;
		job.setOutputFormat(TextOutputFormat.class);
		job.setNumReduceTasks(reducer);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		JobClient.runJob(job);
		return 0;
	}

	/**
	 * check the args
	 */
	private void checkArgs() {
		if (input == null || "".equals(input)) {
			System.out.println("no input...");
			printUsage();
			System.exit(-1);
		}
		if (output == null || "".equals(output)) {
			System.out.println("no output...");
			printUsage();
			System.exit(-1);
		}
		if (delimiter == null || "".equals(delimiter)) {
			System.out.println("no delimiter...");
			printUsage();
			System.exit(-1);
		}
		if (reducer == 0) {
			System.out.println("no reducer...");
			printUsage();
			System.exit(-1);
		}
	}

	/**
	 * configuration the args
	 * -i=/home/wangwei/opt/input/chain2 -o=/home/wangwei/opt/output/chain2 -delimiter=, -reducer=2
	 * @param args
	 */
	private void configureArgs(String[] args) {
		for (int i = 0; i < args.length; i++) {
			
			System.out.println("args[i]="+args[i]);
			System.out.println("args[++i]="+args[++i]);
			
			if ("-i".equals(args[i])) {
				input = args[++i];
			}
			if ("-o".equals(args[i])) {
				output = args[++i];
			}

			if ("-delimiter".equals(args[i])) {
				delimiter = args[++i];
			}
			if ("-reducer".equals(args[i])) {
				try {
					reducer = Integer.parseInt(args[++i]);
				} catch (Exception e) {
					reducer = 0;
				}
			}
		}
	}
	
	public static void printUsage() {
		System.err.println("Usage:");
		System.err.println("-i input \t cell data path.");
		System.err.println("-o output \t output data path.");
		System.err
				.println("-delimiter  data delimiter , default is blanket  .");
		System.err.println("-reducer  reducer number , default is 1  .");
	}

}
