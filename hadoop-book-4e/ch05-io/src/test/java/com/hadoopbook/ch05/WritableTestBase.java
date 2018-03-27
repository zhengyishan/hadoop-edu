package com.hadoopbook.ch05;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class WritableTestBase {
	/**
	 * 序列化
	 * 
	 * @param writable
	 * @return
	 * @throws IOException
	 */
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	/**
	 * 反序列化
	 * 
	 * @param writable
	 * @param bytes
	 * @return
	 * @throws IOException
	 */
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}

	public static String serializeToString(Writable src) throws IOException {
		return StringUtils.byteToHexString(serialize(src));
	}

	public static String writeTo(Writable src, Writable dest) throws IOException {
		byte[] data = deserialize(dest, serialize(src));
		return StringUtils.byteToHexString(data);
	}

}
