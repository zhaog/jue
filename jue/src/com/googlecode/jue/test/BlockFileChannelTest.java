/**
 * 
 */
package com.googlecode.jue.test;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.googlecode.jue.file.Adler32ChecksumGenerator;
import com.googlecode.jue.file.BlockFileChannel;
import com.googlecode.jue.file.ChecksumException;

/**
 * @author noah
 *
 */
public class BlockFileChannelTest extends TestCase {
	
	private BlockFileChannel blockFileChannel;
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		blockFileChannel = new BlockFileChannel("blockTestFile", 32, new Adler32ChecksumGenerator());
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		blockFileChannel.close();
	}
	
//	public void testWriteData() {
//		ByteBuffer dataBuffer = ByteBuffer.allocate(100);
//		for (int i = 0; i < 26; ++i) {
//			dataBuffer.put((i + "").getBytes());
//		}
//		dataBuffer.flip();
//		try {
//			blockFileChannel.write(dataBuffer, BlockFileChannel.CHECKSUM_SIZE);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public void testReadData() {
		ByteBuffer dataBuffer = ByteBuffer.allocate(42);
		try {
			int size = blockFileChannel.read(dataBuffer, BlockFileChannel.CHECKSUM_SIZE, true);
//			Assert.assertEquals(16, size);
			dataBuffer.limit(size);
			dataBuffer.flip();
			byte[] array = new byte[size];
			dataBuffer.get(array);
			String s = new String(array);
			Assert.assertEquals("cd", s);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ChecksumException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
