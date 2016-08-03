package com.asiainfo.hadoop.ChainMapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass2 extends Mapper<Text, Text, Text, Text> {

	public void map(Text ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		System.out.println("map2=ivalue="+ivalue.toString());
		String[] citation = ivalue.toString().split(" ");
		if (!ikey.toString().equals("101")) {
			System.out.println("map2=排除101=ivalue="+ivalue.toString());
			context.write(ikey, ivalue);
		}
	}

}
