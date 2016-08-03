package com.asiainfo.hadoop.ChainMapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		System.out.println("license2========"+context.getConfiguration().get("license"));
		System.out.println("sasa========"+context.getConfiguration().get("sasa"));
		
		// process values
		for (Text val : values) {
			System.out.println("reduce=val="+val.toString());
			context.write(null, val);
		}
	}

}
