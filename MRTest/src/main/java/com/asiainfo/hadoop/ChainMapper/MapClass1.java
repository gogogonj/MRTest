package com.asiainfo.hadoop.ChainMapper;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		System.out.println("map1=ivalue="+ivalue.toString());
		String[] citation = ivalue.toString().split(" ");
		if (!citation[0].equals("100")) {
			System.out.println("map1=排除100=ivalue="+ivalue.toString());
			context.write(new Text(citation[0]), new Text(ivalue));
		}
	}

}
