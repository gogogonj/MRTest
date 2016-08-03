package com.bonc.input.newapi;

import com.bonc.common.Crypt;
import com.bonc.input.NewLineReader;
import java.io.IOException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DpiDecodeRecordReader extends RecordReader<LongWritable, Text> {
	
	private static final Log LOG = LogFactory
			.getLog(DpiDecodeRecordReader.class);
	
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private NewLineReader in;
	private int maxLineLength;
	private LongWritable key = null;
	private Text value = null;
	private String licensekey = "";
	private byte[] separator = "\n".getBytes();

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		this.licensekey = job.get("dpi.encode.license");

		this.start = split.getStart();
		this.end = (this.start + split.getLength());
		Path file = split.getPath();
		this.compressionCodecs = new CompressionCodecFactory(job);
		CompressionCodec codec = this.compressionCodecs.getCodec(file);

		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			this.in = new NewLineReader(codec.createInputStream(fileIn), job);
			this.end = Long.MAX_VALUE;
		} else {
			if (this.start != 0L) {
				skipFirstLine = true;
				this.start -= this.separator.length;

				fileIn.seek(this.start);
			}
			this.in = new NewLineReader(fileIn, job);
		}
		if (skipFirstLine) {
			this.start = (this.start + this.in.readLine(new Text(), 0,
					(int) Math.min(2147483647L, this.end - this.start)));
		}
		this.pos = this.start;
	}

	public boolean nextKeyValue() throws IOException {
		if (this.key == null) {
			this.key = new LongWritable();
		}
		if (this.value == null) {
			this.value = new Text();
		}
		int newSize = 0;
		while (this.pos < this.end) {
			Text tempText = new Text();

			newSize = this.in.readLine(tempText, this.maxLineLength, Math.max(
					(int) Math.min(2147483647L, this.end - this.pos),
					this.maxLineLength));

			byte[] allbytes = Crypt.parseHexStr2Byte(tempText.toString());
			if (allbytes != null) {
				try {
					KeyGenerator kgen = KeyGenerator.getInstance("AES");
					SecureRandom secureRandom = SecureRandom
							.getInstance("SHA1PRNG");
					secureRandom.setSeed(this.licensekey.getBytes());
					kgen.init(128, secureRandom);
					SecretKey secretKey = kgen.generateKey();
					byte[] enCodeFormat = secretKey.getEncoded();
					SecretKeySpec key = new SecretKeySpec(enCodeFormat, "AES");
					Cipher cipher = Cipher.getInstance("AES");
					cipher.init(2, key);
					allbytes = cipher.doFinal(allbytes);
				} catch (Exception e) {
					e.printStackTrace();
				}
				this.value.set(new String(allbytes, "UTF-8"));
			}
			if (newSize == 0) {
				break;
			}
			this.pos += newSize;
			if (newSize < this.maxLineLength) {
				break;
			}
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (this.pos - newSize));
		}
		if (newSize == 0) {
			this.key = null;
			this.value = null;
			return false;
		}
		return true;
	}

	public LongWritable getCurrentKey() {
		return this.key;
	}

	public Text getCurrentValue() {
		return this.value;
	}

	public float getProgress() {
		if (this.start == this.end) {
			return 0.0F;
		}
		return Math.min(1.0F, (float) (this.pos - this.start)
				/ (float) (this.end - this.start));
	}

	public synchronized void close() throws IOException {
		if (this.in != null) {
			this.in.close();
		}
	}
}
