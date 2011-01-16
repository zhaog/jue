/**
 * 
 */
package com.googlecode.jue.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.Checksum;

import com.googlecode.jue.util.LRUCache;

/**
 * 以块的方式读写文件，可以设置块的大小，以及是否缓存
 * @author noah
 */
public class BlockFileChannel {
	
	/**
	 * 最大缓存数
	 */
	public static final int MAX_CAPACITY = 100000;
	
	/**
	 * 文件块的校验码的长度
	 */
	public static final int CHECKSUM_SIZE = 8;
	
	/**
	 * 文件Channel对象
	 */
	private FileChannel fileChannel;
	
	/**
	 * 块大小
	 */
	private final int blockSize;
	
	/**
	 * 文件块中实际存储的数据大小
	 */
	private final int blockDataSize;
	
	/**
	 * 是否缓存文件块
	 */
	private boolean blockCache;
	
	/**
	 * 缓存
	 */
	private LRUCache<Long, ByteBuffer> cache;

	/**
	 * 校验码生成器
	 */
	private ChecksumGenerator checksumGenerator;
	/**
	 * 构造一个BlockFileChannel
	 * @param file 文件对象
	 * @param blockSize 快大小
	 * @param checksumGenerator 校验码生成器
	 * @throws FileNotFoundException
	 */
	public BlockFileChannel(File file, int blockSize, ChecksumGenerator checksumGenerator) throws FileNotFoundException {
		this(file, blockSize, false, checksumGenerator);
	}
	
	/**
	 * 构造一个BlockFileChannel
	 * @param filePath 文件路径
	 * @param blockSize 快大小
	 * @param checksumGenerator 校验码生成器
	 * @throws FileNotFoundException
	 */
	public BlockFileChannel(String filePath, int blockSize, ChecksumGenerator checksumGenerator) throws FileNotFoundException {
		this(filePath, blockSize, false, checksumGenerator);
	}
	
	/**
	 * 构造一个BlockFileChannel
	 * @param filePath 文件路径
	 * @param blockSize 块大小
	 * @param blockCache 是否缓存块
	 * @param checksumGenerator 校验码生成器
	 * @throws FileNotFoundException 文件不存在
	 */
	public BlockFileChannel(String filePath, int blockSize, boolean blockCache, ChecksumGenerator checksumGenerator) throws FileNotFoundException {
		this(new File(filePath), blockSize, blockCache, checksumGenerator);
	}

	/**
	 * 构造一个BlockFileChannel
	 * @param file 文件对象
	 * @param blockSize 块大小
	 * @param blockCache 是否缓存块
	 * @param checksumGenerator 校验码生成器
	 * @throws FileNotFoundException 文件不存在
	 */
	public BlockFileChannel(File file, int blockSize, boolean blockCache, ChecksumGenerator checksumGenerator) throws FileNotFoundException {
		super();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		fileChannel = raf.getChannel();
		this.blockSize = blockSize;
		this.blockDataSize = blockSize - CHECKSUM_SIZE;
		// 文件块小于校验码大小
		if (this.blockDataSize <= 0) {
			throw new IllegalArgumentException("too small block size");
		}
		this.blockCache = blockCache;
		this.checksumGenerator = checksumGenerator;
		if (blockCache) {
			cache = new LRUCache<Long, ByteBuffer>(MAX_CAPACITY);
		}
	}
	
	/**
	 * 关闭文件
	 * @throws IOException
	 */
	public void close() throws IOException {
		fileChannel.close();
	}
	
	/**
	 * 返回文件大小
	 * @return
	 * @throws IOException
	 */
	public long size() throws IOException {
		return fileChannel.size();
	}
	
	/**
	 * 读取数据到字节数组中
	 * @param buffer 需要存储的数据缓存
	 * @param position 读取位置
	 * @param checksum 是否要校验数据
	 * @return 返回读取的数据长度
	 * @throws IOException 文件读取异常
	 * @throws ChecksumException 校验错误抛出的异常
	 */
	public int read(ByteBuffer buffer, long position, boolean checksum) throws IOException, ChecksumException {
		// 文件为空
		if(size() == 0) {
			return -1;
		}
		long mod = position % blockSize;
		// 读取位置处于校验码的位置
		if (mod < CHECKSUM_SIZE) {
			throw new IllegalArgumentException("can not read checksum data");
		}
		// 最大读取的数据量
		int maxSize = buffer.remaining();
		// 读取的数据长度
		int count = 0;
		// 获取需要读取的数据对应的文件块的位置
		long[] blockIndexes = getReadBlockIndexes(position, maxSize);
		for (int i = 0; i < blockIndexes.length; ++i) {
			ByteBuffer blockDataBuffer = getBlockData(blockIndexes[i], checksum);
			if (i == 0) {
				// 要从数据块中读取的起始位置
				blockDataBuffer.position((int) (mod - CHECKSUM_SIZE));
				// 设置第一个block块的读书数据大小限制
				if (maxSize < blockDataBuffer.remaining()) {
					blockDataBuffer.limit(blockDataBuffer.position() + maxSize);
				}
			} else if (i == blockIndexes.length - 1) {// 最后一块文件块，未必需要读取全部
				// 需要读取的剩余大小
				int s = maxSize - count;
				if (s < blockDataBuffer.remaining()) {
					blockDataBuffer.limit(s);
				}
			}
			// 更新已经读取的数据量
			count += blockDataBuffer.remaining();
			buffer.put(blockDataBuffer);
			
		}
		return count;
	}
	
	/**
	 * 读取相应的文件块
	 * @param blockIndex 文件块位置
	 * @param checksum 是否需要校验数据
	 * @return
	 * @throws IOException 
	 * @throws ChecksumException 
	 */
	private ByteBuffer getBlockData(long blockIndex, boolean checksum) throws IOException, ChecksumException {
		ByteBuffer dataBuffer = null;
		// 从缓存中获取该文件块
		if (blockCache) {
			dataBuffer = cache.get(blockIndex);
		}
		// 缓存中不存在该块
		if (dataBuffer == null) {
			// 创建缓冲区
			ByteBuffer buffer = ByteBuffer.allocate(blockSize);
			// 已经读取的大小
			int ct = 0;
			do {
				// 读取数据
				int n = fileChannel.read(buffer, blockIndex * blockSize + ct);
				// 读到末尾
				if (n == -1) {
					break;
				}
				ct += n;
			} while (ct < blockSize);// 直到读满一个文件块的容量
			// 未读取到任何数据
			if (ct == 0) {
				return null;
			}
			buffer.flip();
			// 校验码
			long chsum = buffer.getLong();
			// 获取实际的数据
			byte[] d = new byte[buffer.remaining()];
			buffer.get(d);
			// 是否需要校验
			if (checksum) {
				Checksum cksum = checksumGenerator.createChecksum();
				cksum.update(d, 0, d.length);
				long chsum2 = cksum.getValue();
				// 校验码错误
				if (chsum != chsum2) {
					throw new ChecksumException();
				}
			}
			// 只包含数据的数据缓冲区
			dataBuffer = ByteBuffer.allocate(blockSize - CHECKSUM_SIZE);
			buffer.position(CHECKSUM_SIZE);
			dataBuffer.put(buffer);
			if (blockCache) {
				// 存入缓存
				cache.put(blockIndex, dataBuffer);
			}
		}
		dataBuffer.rewind();
		return dataBuffer;
	}

	/**
	 * 获取需要读取的数据对应的文件块的位置
	 * @param pos 文件位置
	 * @param size 要获取的长度
	 * @return
	 * @throws IOException 
	 */
	private long[] getReadBlockIndexes(long pos, int size) throws IOException {
		long fileSize = size();
		// 读取位置大于文件
		if (pos >= fileSize) {
			throw new IOException("out of file");
		}
		// 起始文件块的位置
		long startBlockIndex = pos / blockSize;
		// 最后一个文件块的索引位置
		long lastBlockIndex = (long) Math.ceil((double) fileSize / blockSize) - 1;
		// 确定结束的块位置
		long endBlockIndex = 0;
		long m = blockSize - pos % blockSize;
		long sz = size;
		sz -= m;
		if (sz <= 0) {
			endBlockIndex = startBlockIndex;
		} else {
			// 之后需要再读取的块数
			long c = sz / this.blockDataSize + 1;
			// 结束文件块的位置
			endBlockIndex = startBlockIndex + c;
		}
		
		// 超出文件
		if (endBlockIndex > lastBlockIndex) {
			endBlockIndex = lastBlockIndex;
		}
		// 需要读取的文件块的个数
		int count = (int) (endBlockIndex - startBlockIndex + 1);
		// 获取各文件块位置
		long[] indexes = new long[count];
		for (int i = 0; i < count; ++i, ++startBlockIndex) {
			indexes[i] = startBlockIndex;
		}
		return indexes;
	}
	
	/**
	 * 在指定位置写入数据
	 * @param data 数据缓冲区
	 * @param position 位置
	 * @return
	 * @throws IOException
	 */
	public int write(ByteBuffer dataBuffer, long position) throws IOException {
		long mod = position % blockSize;
		// 读取位置处于校验码的位置
		if (mod < CHECKSUM_SIZE) {
			throw new IllegalArgumentException("can not write checksum data");
		}
		// 文件大小
		long fileSize = size();
		// 写入的位置和文件尾部之间，超过了一个block块
		if (position - fileSize >= blockSize) {
			throw new IOException("out of file block");
		}
		// 需要写入的数据长度
		int dataSize = dataBuffer.remaining();
		// 保存缓冲区限制
		int oldLimit = dataBuffer.limit();
		// 已经写入的数据长度
		int written = 0;
		long[] blockIndexes = getWriteBlockIndexes(position, dataSize);
		for (int i = 0; i < blockIndexes.length; ++i) {
			long blockIndex = blockIndexes[i];
			ByteBuffer blockDataBuffer = null;
			if (blockIndex * blockSize < fileSize) {// 该索引块有数据，将数据读出
				try {
					blockDataBuffer = getBlockData(blockIndex, false);
				} catch (ChecksumException e) {// 不校验数据，不会抛出异常
				}
			} else {
				blockDataBuffer = ByteBuffer.allocate(blockSize - CHECKSUM_SIZE);
			}
			if (i == 0) {
				// 第一个块的可写部分大小
				int limit = (int) (blockSize - mod);
				// 设置可以写入第一个block块的数据大小限制
				if (dataBuffer.remaining() > limit) {
					dataBuffer.limit(limit);
				}
				blockDataBuffer.position((int) (mod - CHECKSUM_SIZE));
			} else if (i == blockIndexes.length - 1) { // 最后一块文件块，将所有数据写入
				dataBuffer.limit(oldLimit);
			} else {
				dataBuffer.limit(dataBuffer.position() + blockDataSize);
			}
			// 更新写入的数据长度
			written += dataBuffer.remaining();
			blockDataBuffer.put(dataBuffer);
			blockDataBuffer.rewind();
			writeBlockData(blockIndex, blockDataBuffer);
		}
		return written;
	}
	
	/**
	 * 写入块数据
	 * @param blockIndex
	 * @param dataBuffer
	 * @throws IOException
	 */
	private void writeBlockData(long blockIndex, ByteBuffer dataBuffer) throws IOException {
		Checksum checksum = checksumGenerator.createChecksum();
		byte[] b = new byte[dataBuffer.remaining()];
		dataBuffer.get(b);
		checksum.update(b, 0, b.length);
		// 生成校验码
		long chksum = checksum.getValue();
		ByteBuffer buffer = ByteBuffer.allocate(blockSize);
		buffer.putLong(chksum);
		dataBuffer.rewind();
		buffer.put(dataBuffer);
		buffer.flip();
		fileChannel.write(buffer, blockIndex * blockSize);
	}

	/**
	 * 获取即将写入的数据对应的文件块的位置
	 * @param pos 文件位置
	 * @param size 要获取的长度
	 * @return
	 * @throws IOException 
	 */
	private long[] getWriteBlockIndexes(long pos, int size) throws IOException {
		// 起始文件块的位置
		long startBlockIndex = pos / blockSize;
		// 确定结束的块位置
		long endBlockIndex = 0;
		long m = blockSize - pos % blockSize;
		long sz = size;
		sz -= m;
		if (sz <= 0) {
			endBlockIndex = startBlockIndex;
		} else {
			// 之后需要再读取的块数
			int count = (int) Math.ceil( (double)sz / this.blockDataSize);
			// 结束文件块的位置
			endBlockIndex = startBlockIndex + count;
		}
		// 需要读取的文件块的个数
		int count = (int) (endBlockIndex - startBlockIndex + 1);
		// 获取各文件块位置
		long[] indexes = new long[count];
		for (int i = 0; i < count; ++i, ++startBlockIndex) {
			indexes[i] = startBlockIndex;
		}
		return indexes;
	}
}
