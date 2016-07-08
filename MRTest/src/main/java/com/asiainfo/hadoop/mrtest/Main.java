package com.asiainfo.hadoop.mrtest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

// hadoop jar /home/wangwei/opt/test.jar com.asiainfo.hadoop.mrtest.Main /home/wangwei/opt/input/in /home/wangwei/opt/input/in2 /home/wangwei/opt/output
public class Main {

	public static void main(String[] args) {

		try {
			int i = ToolRunner.run(new Configuration(), new Tools(), args);
			System.exit(i);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
