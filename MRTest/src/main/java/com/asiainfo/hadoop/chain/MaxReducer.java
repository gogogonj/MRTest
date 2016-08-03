package com.asiainfo.hadoop.chain;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaxReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {
	private Logger log = LoggerFactory.getLogger(MaxReducer.class);

	@Override
	public void configure(JobConf conf) {
		
	}

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> out, Reporter reporter)
			throws IOException {
		
		int max = -1;
		while (values.hasNext()) {
			int value = values.next().get();
			if (value > max) {
				max = value;
			}
		}
		
		out.collect(key, new IntWritable(max));

	}

	@Override
	public void close() {
		
	}
}
