package com.asiainfo.hadoop.mrtest3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

class DpiLabelMapper extends Mapper<LongWritable, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;

	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context) {
		try {
			mos.close();
		} catch (IOException e) {
		} catch (InterruptedException e) {
		}
	}

	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context) {
		try {
			String[] line = value.toString().split(",");
			context.write(new Text(line[0]), value);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
