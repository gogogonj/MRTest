package com.bonc.input;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class NewLineReader {
	private static final int DEFAULT_BUFFER_SIZE = 65536;
	private int bufferSize = 65536;
	private InputStream in;
	private byte[] buffer;
	private int bufferLength = 0;
	private int bufferPosn = 0;
	private byte[] separator = "\n".getBytes();

	public NewLineReader(InputStream in) {
		this(in, 65536);
	}

	public NewLineReader(InputStream in, int bufferSize) {
		this.in = in;
		this.bufferSize = bufferSize;
		this.buffer = new byte[this.bufferSize];
	}

	public NewLineReader(InputStream in, Configuration conf) throws IOException {
		this(in, conf.getInt("io.file.buffer.size", 65536));
	}

	public void close() throws IOException {
		this.in.close();
	}

	public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
			throws IOException {
		str.clear();
		Text record = new Text();
		int txtLength = 0;
		long bytesConsumed = 0L;
		boolean newline = false;
		int sepPosn = 0;
		do {
			if (this.bufferPosn >= this.bufferLength) {
				this.bufferPosn = 0;
				this.bufferLength = this.in.read(this.buffer);
				if (this.bufferLength <= 0) {
					break;
				}
			}
			int startPosn = this.bufferPosn;
			for (; this.bufferPosn < this.bufferLength; this.bufferPosn += 1) {
				if ((sepPosn > 0)
						&& (this.buffer[this.bufferPosn] != this.separator[sepPosn])) {
					sepPosn = 0;
				}
				if (this.buffer[this.bufferPosn] == this.separator[sepPosn]) {
					this.bufferPosn += 1;
					int i = 0;
					for (sepPosn++; sepPosn < this.separator.length; sepPosn++) {
						if (this.bufferPosn + i >= this.bufferLength) {
							this.bufferPosn += i - 1;
							break;
						}
						if (this.buffer[(this.bufferPosn + i)] != this.separator[sepPosn]) {
							sepPosn = 0;
							break;
						}
						i++;
					}
					if (sepPosn == this.separator.length) {
						this.bufferPosn += i;
						newline = true;
						sepPosn = 0;
						break;
					}
				}
			}
			int readLength = this.bufferPosn - startPosn;
			bytesConsumed += readLength;
			if (readLength > maxLineLength - txtLength) {
				readLength = maxLineLength - txtLength;
			}
			if (readLength > 0) {
				record.append(this.buffer, startPosn, readLength);
				txtLength += readLength;
				if (newline) {
					str.set(record.getBytes(), 0, record.getLength()
							- this.separator.length);
				}
			}
		} while ((!newline) && (bytesConsumed < maxBytesToConsume));
		if (bytesConsumed > 2147483647L) {
			throw new IOException("Too many bytes before newline: "
					+ bytesConsumed);
		}
		return (int) bytesConsumed;
	}

	public int readLine(Text str, int maxLineLength) throws IOException {
		return readLine(str, maxLineLength, Integer.MAX_VALUE);
	}

	public int readLine(Text str) throws IOException {
		return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
	}
}
