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

import com.googlecode.jue.LRUCache;

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
	 * 是否缓存文件块
	 */
	private boolean blockCache;
	
	/**
	 * 缓存
	 */
	private LRUCache<Long, byte[]> cache;

	/**
	 * 构造一个BlockFileChannel
	 * @param file 文件对象
	 * @param blockSize 快大小
	 * @throws FileNotFoundException
	 */
	public BlockFileChannel(File file, int blockSize) throws FileNotFoundException {
		this(file, blockSize, false);
	}
	
	/**
	 * 构造一个BlockFileChannel
	 * @param filePath 文件路径
	 * @param blockSize 快大小
	 * @throws FileNotFoundException
	 */
	public BlockFileChannel(String filePath, int blockSize) throws FileNotFoundException {
		this(filePath, blockSize, false);
	}
	
	/**
	 * 构造一个BlockFileChannel
	 * @param filePath 文件路径
	 * @param blockSize 块大小
	 * @param blockCache 是否缓存块
	 * @throws FileNotFoundException 文件不存在
	 */
	public BlockFileChannel(String filePath, int blockSize, boolean blockCache) throws FileNotFoundException {
		this(new File(filePath), blockSize, blockCache);
	}
	
	/**
	 * 构造一个BlockFileChannel
	 * @param file 文件对象
	 * @param blockSize 块大小
	 * @param blockCache 是否缓存块
	 * @throws FileNotFoundException 文件不存在
	 */
	public BlockFileChannel(File file, int blockSize, boolean blockCache) throws FileNotFoundException {
		super();
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		fileChannel = raf.getChannel();
		this.blockSize = blockSize;
		this.blockCache = blockCache;
		if (blockCache) {
			cache = new LRUCache<Long, byte[]>(MAX_CAPACITY);
		}
	}
	
	public int read(byte[] data, boolean checksum) throws IOException {
		// 当前的文件位置
		long pos = fileChannel.position();
		// 需要读取的数据量
		int size = data.length;
		// 获取需要读取的数据对应的文件块的位置
		long[] blockIndexes = getBlockIndexes(pos, size);
		for (int i = 0; i < blockIndexes.length; ++i) {
			byte[] b = getBlockData(blockIndexes[i], checksum);
			// TODO 将读出的数据拷贝到data数组中
		}
		return 0;
	}
	
	/**
	 * 读取相应的文件块
	 * @param blockIndex 文件块位置
	 * @param checksum 是否需要校验数据
	 * @return
	 * @throws IOException 
	 */
	private byte[] getBlockData(long blockIndex, boolean checksum) throws IOException {
		byte[] data = null;
		// 从缓存中获取该文件块
		if (blockCache) {
			data = cache.get(blockIndex);
		}
		// 缓存中不存在该块
		if (data == null) {
			// 创建缓冲区
			ByteBuffer buffer = ByteBuffer.allocate(blockSize);
			// 已经读取的大小
			int ct = 0;
			do {
				// 读取数据
				int n = fileChannel.read(buffer, blockIndex + ct);
				// 读到末尾
				if (n == -1) {
					break;
				}
				ct += n;
			} while (ct < blockSize);// 直到读满一个文件块的容量
			buffer.flip();
			// 校验码
			long chsum = buffer.getLong();
			// 获取实际的数据
			data = new byte[buffer.remaining()];
			buffer.get(data);
			// 是否需要校验
			if (checksum) {
				// TODO 执行校验
			}
			
		}
		return data;
	}

	/**
	 * 获取需要读取的数据对应的文件块的位置
	 * @param pos 文件位置
	 * @param size 要获取的长度
	 * @return
	 */
	private long[] getBlockIndexes(long pos, int size) {
		// 起始文件块的位置
		long startBlockIndex = pos / blockSize;
		// 数据的结束位置
		long endPos = pos + size;
		// 结束文件块的位置
		long endBlockIndex = endPos / blockSize;
		// 需要读取的文件块的个数
		int count = (int) (endBlockIndex - startBlockIndex + 1);
		// 获取各文件块位置
		long[] indexes = new long[count];
		for (int i = 0; i < count; ++i, ++startBlockIndex) {
			indexes[i] = startBlockIndex;
		}
		return indexes;
	}

	public int read(ByteBuffer dst, long position) throws IOException {
		return fileChannel.read(dst, position);
	}
	
}
