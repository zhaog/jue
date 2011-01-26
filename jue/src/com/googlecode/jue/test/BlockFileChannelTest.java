/**
 * 
 */
package com.googlecode.jue.test;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.googlecode.jue.file.CRC32ChecksumGenerator;
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
		blockFileChannel = new BlockFileChannel("blockTestFile", 32, new CRC32ChecksumGenerator());
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		blockFileChannel.close();
	}
	
//	public void testWriteData() {
//		ByteBuffer dataBuffer = ByteBuffer.allocate(100);
//		for (int i = 0; i < 10; ++i) {
//			dataBuffer.putInt(i);
//		}
//		dataBuffer.flip();
//		try {
//			blockFileChannel.write(dataBuffer, 0);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public void testReadData() {
		ByteBuffer dataBuffer = ByteBuffer.allocate(40);
		try {
			int size = blockFileChannel.read(dataBuffer, 0, true);
			Assert.assertEquals(40, size);
			dataBuffer.limit(size);
			dataBuffer.flip();
			for (int i = 0; i < 10; ++i) {
				int j = dataBuffer.getInt();
				Assert.assertEquals(i, j);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ChecksumException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
