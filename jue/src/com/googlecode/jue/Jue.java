/**
 * 
 */
package com.googlecode.jue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.googlecode.jue.bplustree.CopyOnWriteBPlusTree;
import com.googlecode.jue.doc.DocObject;
import com.googlecode.jue.exception.RevisionInvalidException;
import com.googlecode.jue.file.ADrop;
import com.googlecode.jue.file.BlockFileChannel;
import com.googlecode.jue.file.CRC32ChecksumGenerator;
import com.googlecode.jue.file.ChecksumException;
import com.googlecode.jue.file.DropTransfer;
import com.googlecode.jue.file.FileHeader;
import com.googlecode.jue.file.KeyRecord;
import com.googlecode.jue.util.ConcurrentLRUCache;

/**
 * Jue的主类，创建文件，操作文件等都是通过这个类来执行的。
 * @author noah
 *
 */
public class Jue {

	/**
	 * 文件锁
	 */
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * 读锁
	 */
	private final ReadLock readLock = lock.readLock();
	
	/**
	 * 写锁
	 */
	private final WriteLock writeLock = lock.writeLock();
	
	/**
	 * 缓存
	 */
	private final ConcurrentLRUCache<String, CacheObject> cache = new ConcurrentLRUCache<String, CacheObject>();
	
	/**
	 * key的树索引
	 */
	private CopyOnWriteBPlusTree<String, KeyRecord> keyTree;
	
	/**
	 * 文件头信息
	 */
	private FileHeader fileHeader;
	
	private DropTransfer dropTransfer;
	
	private BlockFileChannel blockFileChannel;
	
	/**
	 * 打开或者创建Jue
	 * @param filePath
	 */
	public Jue(String filePath) {
		this(filePath, new FileConfig());
	}
	
	/**
	 * 打开或者创建Jue
	 * @param filePath
	 * @param config
	 */
	public Jue(String filePath, FileConfig config) {
		File file = new File(filePath);
		boolean exist = file.exists();
		blockFileChannel = new BlockFileChannel(file, config.getBlockSize(), config.isBlockCache(), new CRC32ChecksumGenerator());
		dropTransfer = new DropTransfer(blockFileChannel);
		try {
			int keyTreeMin = 0;
			if (!exist) {
				fileHeader = createEmptyFile(config);
			} else {
				fileHeader = dropTransfer.readHeader();
			}
			keyTreeMin = fileHeader.getKeyTreeMin();
			initTree(keyTreeMin);
			initCache();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}

	/**
	 * 读取文件，初始化树索引
	 * @param keyTreeMin
	 */
	private void initTree(int keyTreeMin) {
		keyTree = new CopyOnWriteBPlusTree<String, KeyRecord>(keyTreeMin);
		// TODO init tree
	}

	private void initCache() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 创建文件，并添加文件头
	 * @param config
	 * @return 
	 * @throws IOException 
	 */
	private FileHeader createEmptyFile(FileConfig config) throws IOException {
		FileHeader header = new FileHeader();
		header.setFileTail(0x0);
		header.setKeyTreeMin(config.getKeyTreeMin());
		byte valueCompressed = ADrop.FALSE_BYTE;
		byte compressionCodec = (byte) FileConfig.CompressionType.NOT_COMPRESSED.ordinal();
		if (config.isValueCompressed()) {
			valueCompressed = ADrop.TRUE_BYTE;
			compressionCodec = (byte) config.getCompressionType().ordinal();
		}
		header.setValueCompressed(valueCompressed);
		header.setCompressionCodec(compressionCodec);
		header.setValueRevTreeMin(config.getValueRevTreeMin());
		ByteBuffer buffer = dropTransfer.headerToByteBuffer(header);
		blockFileChannel.write(buffer, 0);
		return header;
	}
	
	/**
	 * Put一个Doc对象，如果版本号小于零，那么就忽略该版本要求
	 * @param key 主键
	 * @param docObj 文档对象
	 * @param requireRev 该操作基于的版本号
	 * @return 返回操作成功后，数据的版本号
	 */
	public int put(String key, DocObject docObj, int requireRev) {
		if (requireRev >=0) {
			checkRev(key, requireRev);
		}
		
		// TODO do Put
		
		return 0;
	}

	private void checkRev(String key, int requireRev) {
		int currentRev = getCurrentRev(key);
		if (currentRev >= 0 && currentRev != requireRev) {
			throw new RevisionInvalidException();
		}
	}

	/**
	 * 获取key对应的当前版本号，如果该key不存在，则返回-1
	 * @param key
	 * @return key对应的当前版本号，或者key不存在则返回-1
	 */
	private int getCurrentRev(String key) {
		// 从缓存获取当前记录
		CacheObject cacheObj = cache.get(key);
		if (cacheObj != null) {
			return cacheObj.currentRev;
		}
		// 缓存不存在，从索引中获取key信息
		KeyRecord record = keyTree.get(key);
		if (record != null) {
			return record.getRevision();
		}
		// 该key不存在，返回-1
		return -1;
	}
	
	/**
	 * 缓存对象
	 * @author noah
	 *
	 */
	private class CacheObject {
		/**
		 * 当前版本号
		 */
		int currentRev;
		
		/**
		 * 文档对象
		 */
		DocObject docObj;
	}
}
