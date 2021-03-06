package com.asiainfo.hadoop.mrtest2;

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
		String score1 = "";
		String score2 = "";
		for (Text value : values) {
			String [] args = value.toString().split(",");
			if(args[1].equals("1")){
				score1 = args[2];
			}else if(args[1].equals("2")){
				score2 = args[2];
			}
		}
		String result = "";
		if(score1.equals("10") && score2.equals("50")){
			result = key.toString()+",aaaaa";
		}else{
			result = key.toString()+",bbbbb";
		}
		this.mos.write("NullKey", null, new Text(result), "DpiLabel-13");
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
