/**
 * 
 */
package com.googlecode.jue.file;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ADrop的读写类
 * @author noah
 *
 */
public class DropChannel {
	
	/**
	 * 块读取Channel
	 */
	private BlockFileChannel blockChannel;

	/**
	 * 创建一个DropChannel
	 * @param blockChannel
	 */
	public DropChannel(BlockFileChannel blockChannel) {
		super();
		this.blockChannel = blockChannel;
	}
	
	/**
	 * 读取文件头
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	public FileHeader readHeader() throws IOException, ChecksumException {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);
		blockChannel.read(buffer, BlockFileChannel.CHECKSUM_SIZE, true);
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
	
	/**
	 * 写文件头
	 * @param header
	 * @throws IOException 
	 */
	public void wirteHeader(FileHeader header) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);		
		
		buffer.putLong(header.getFileTail());
		buffer.putInt(header.getKeyTreeMin());
		buffer.put(header.getKeyType());
		buffer.putInt(header.getValueRevTreeMin());
		buffer.put(header.getValueRevType());
		buffer.put(header.getValueCompressed());
		buffer.put(header.getCompressionCodec());
		buffer.flip();
		blockChannel.write(buffer, BlockFileChannel.CHECKSUM_SIZE);
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
		blockChannel.read(buffer, BlockFileChannel.CHECKSUM_SIZE, true);
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
	 * 在文件末尾添加文件尾
	 * @param tail
	 * @return 返回添加的位置
	 * @throws IOException
	 */
	public long appendTail(FileTail tail) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);
	    
	    buffer.putInt(tail.getRevision());
	    buffer.putLong(tail.getRootNode());
	    buffer.putInt(tail.getAvgKeyLen());
	    buffer.putInt(tail.getAvgValueLen());
	    buffer.putInt(tail.getEntryCount());
	    buffer.flip();
	    long position = blockChannel.size() + BlockFileChannel.CHECKSUM_SIZE;
		blockChannel.write(buffer, position);
		return position;
	}
}
