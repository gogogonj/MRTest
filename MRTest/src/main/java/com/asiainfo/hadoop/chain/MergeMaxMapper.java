package com.asiainfo.hadoop.chain;

import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeMaxMapper extends MapReduceBase implements
		Mapper<Text, IntWritable, Text, IntWritable> {

	private Logger log = LoggerFactory.getLogger(MergeMaxMapper.class);

	@Override
	public void configure(JobConf conf) {
	}

	@Override
	public void map(Text key, IntWritable value,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		
		out.collect(new Text(key.toString() + "_MergeMaxMapper"), value);

	}

	@Override
	public void close() {
	}
}
