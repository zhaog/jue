/**
 * 
 */
package com.googlecode.jue.file;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.googlecode.jue.util.ByteDynamicArray;
import com.googlecode.jue.util.ByteUtil;

/**
 * ADrop的转换读取类
 * @author noah
 *
 */
public class DropTransfer {
	
	/**
	 * 块读取Channel
	 */
	private BlockFileChannel blockChannel;

	/**
	 * 创建一个DropTransfer
	 * @param blockChannel
	 */
	public DropTransfer(BlockFileChannel blockChannel) {
		super();
		this.blockChannel = blockChannel;
	}
	
	/**
	 * 将文件头转换成ByteBuffer
	 * @param header
	 * @return
	 * @throws IOException
	 */
	public ByteBuffer headerToByteBuffer(FileHeader header) {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);		
		
		buffer.putLong(header.getFileTail());
		buffer.putInt(header.getKeyTreeMin());
		buffer.put(header.getKeyType());
		buffer.putInt(header.getValueRevTreeMin());
		buffer.put(header.getValueRevType());
		buffer.put(header.getValueCompressed());
		buffer.put(header.getCompressionCodec());
		buffer.flip();
		
		return buffer;
	}
	
	/**
	 * 文件尾转换成ByteBuffer
	 * @param tail
	 * @return
	 * @throws IOException
	 */
	public ByteBuffer tailToByteBuffer(FileTail tail) {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);
	    
	    buffer.putInt(tail.getRevision());
	    buffer.putLong(tail.getRootNode());
	    buffer.putInt(tail.getAvgKeyLen());
	    buffer.putInt(tail.getAvgValueLen());
	    buffer.putInt(tail.getEntryCount());
	    buffer.flip();
	
		return buffer;
	}

	public ByteBuffer keyNodetoByteBuffer(KeyNode keyNode) {
		ByteDynamicArray array = new ByteDynamicArray();
		// 是否叶节点
		array.add(keyNode.getLeaf());
		
		byte[][] keys = keyNode.getKeys();
		// 关键字的数量
		array.add(ByteUtil.int2byte(keys.length));
		for (int i = 0; i < keys.length; ++i) {
			byte[] key = keys[i];
			// 键的长度
			array.add(ByteUtil.int2byte(key.length));
			// 键的内容
			array.add(key);
		}
		// 添加子树地址
		long[] childAddr = keyNode.getChildOrKeyAddr();
		for (int i = 0; i < childAddr.length; ++i) {
			array.add(ByteUtil.long2byte(childAddr[i]));
		}
		byte[] b = array.toByteArray();
		ByteBuffer buffer = ByteBuffer.allocate(b.length);
		buffer.put(b);
		return buffer;
	}
	
	/**
	 * 读取文件尾
	 * @param position
	 * @return
	 * @throws IOException
	 * @throws ChecksumException
	 */
	public FileTail readTail(long position) throws IOException, ChecksumException {
		ByteBuffer buffer = ByteBuffer.allocate(FileTail.TAIL_LENGHT);
		blockChannel.read(buffer, position, true);
		buffer.flip();
		int revision = buffer.getInt();
		long rootNode = buffer.getLong();
	    int avgKeyLen = buffer.getInt();
	    int avgValueLen = buffer.getInt();
	    int entryCount = buffer.getInt();
	    
	    FileTail tail = new FileTail();
	    tail.setRevision(revision);
	    tail.setRootNode(rootNode);
	    tail.setAvgKeyLen(avgKeyLen);
	    tail.setAvgValueLen(avgValueLen);
	    tail.setEntryCount(entryCount);
		return tail;
	}
	
	/**
	 * 读取文件头
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	public FileHeader readHeader() throws IOException, ChecksumException {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);
		blockChannel.read(buffer, 0, true);
		buffer.flip();
		long fileTail = buffer.getLong();
		int keyTreeMin = buffer.getInt();
		byte keyType = buffer.get();
		int valueRevTreeMin = buffer.getInt();
		byte valueRevType = buffer.get();
		byte valueCompressed = buffer.get();
		byte compressionCodec = buffer.get();
		
		FileHeader header = new FileHeader();
		header.setFileTail(fileTail);
		header.setKeyTreeMin(keyTreeMin);
		header.setKeyType(keyType);
		header.setValueRevTreeMin(valueRevTreeMin);
		header.setValueRevType(valueRevType);
		header.setValueCompressed(valueCompressed);
		header.setCompressionCodec(compressionCodec);
		
		return header;
	}
	
	
}
