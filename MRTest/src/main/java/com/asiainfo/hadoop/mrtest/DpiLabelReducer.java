package com.asiainfo.hadoop.mrtest;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

class DpiLabelReducer extends Reducer<Text, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;

	protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context) {
		this.mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	public void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			this.mos.write("NullKey", null, value, "DpiLabel-13");
		}
	}

	protected void cleanup(
			Reducer<Text, Text, NullWritable, Text>.Context context) {
		try {
			this.mos.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
