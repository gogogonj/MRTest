package com.bonc.input.newapi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DpiDecodeInputFormat extends FileInputFormat<LongWritable, Text> {

	private CompressionCodecFactory compressionCodecs = null;

	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		if (context.getConfiguration().get("dpi.encode.license") == null) {
			System.out
					.println("Error:dpi.encode.license is null,Please set dpi.encode.license!!!!");
			return null;
		}
		return new DpiDecodeRecordReader();
	}

	protected boolean isSplitable(JobContext context, Path filename) {
		this.compressionCodecs = new CompressionCodecFactory(
				context.getConfiguration());
		return this.compressionCodecs.getCodec(filename) == null;
	}
}
