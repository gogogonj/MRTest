package com.asiainfo.hadoop.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//hadoop jar /home/wangwei/opt/testX.jar com.asiainfo.hadoop.FSDataInputStream.Main
// 读取hdfs上的配置文件
public class Main {

	private static Logger logger=LoggerFactory.getLogger(Main.class); 
	
	public static void main(String[] args) {
		
		try {
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(config);
			Path path = new Path("/home/wangwei/opt/input/in");
			FSDataInputStream is = fs.open(path);
			
			BufferedReader br;
			String line;
			try {
				br = new BufferedReader(new InputStreamReader(is));
				while ((line = br.readLine()) != null) {
					System.out.println("===="+line);
					logger.info("===="+line);
				}
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
		}
	}

}
