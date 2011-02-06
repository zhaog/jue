/**
 * 
 */
package com.googlecode.jue;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.googlecode.jue.bplustree.BNode;
import com.googlecode.jue.bplustree.BPlusTree;
import com.googlecode.jue.bplustree.CopyOnWriteBPlusTree;
import com.googlecode.jue.bplustree.BNode.InnerNode;
import com.googlecode.jue.doc.DocObject;
import com.googlecode.jue.exception.RevisionInvalidException;
import com.googlecode.jue.file.ADrop;
import com.googlecode.jue.file.BlockFileChannel;
import com.googlecode.jue.file.CRC32ChecksumGenerator;
import com.googlecode.jue.file.ChecksumException;
import com.googlecode.jue.file.DropTransfer;
import com.googlecode.jue.file.FileHeader;
import com.googlecode.jue.file.FileTail;
import com.googlecode.jue.file.KeyNode;
import com.googlecode.jue.file.KeyRecord;
import com.googlecode.jue.file.ValueRecord;
import com.googlecode.jue.util.ByteDynamicArray;
import com.googlecode.jue.util.ConcurrentLRUCache;
import com.googlecode.jue.util.DocUtils;

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
	private CopyOnWriteBPlusTree<String, Long> keyTree;
	
	/**
	 * 文件头信息
	 */
	private FileHeader fileHeader;
	
	/**
	 * 文件尾
	 */
	private FileTail fileTail;
	
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
			long tailPos = fileHeader.getFileTail();
			long rootNodePos = -1;
			if (tailPos != -1) {
				fileTail = dropTransfer.readTail(tailPos);
				rootNodePos = fileTail.getRootNode();
			}
			keyTreeMin = fileHeader.getKeyTreeMin();
			initTree(keyTreeMin, rootNodePos);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}

	/**
	 * 读取文件，初始化树索引
	 * @param keyTreeMin
	 * @param rootNodeAddr 
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	private void initTree(int keyTreeMin, long rootNodeAddr) throws IOException, ChecksumException {
		keyTree = new CopyOnWriteBPlusTree<String, Long>(keyTreeMin);
		if (rootNodeAddr != -1) {
			BNode<String, Long> rootNode = createKeyBNode(rootNodeAddr, keyTreeMin);
			keyTree.updateNewTree(rootNode);
		}
	}

	/**
	 * 读取文件，创建节点以及遍历创建子节点
	 * @param keyTreeMin 
	 * @param rootNodeAddr
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	private BNode<String, Long> createKeyBNode(long nodePosition, int keyTreeMin) throws IOException, ChecksumException {
		KeyNode keyNode = dropTransfer.readKeyNode(nodePosition);
		boolean isLeaf = keyNode.getLeaf() == KeyNode.TRUE_BYTE;
		BNode<String, Long> node = new BNode<String, Long>(null, keyTreeMin, isLeaf);
		node.setPosition(nodePosition);
		byte[][] keys = keyNode.getKeys();
		node.setCount(keys.length);
		
		if (isLeaf) {
			// 初始化内部节点
			BNode<String , Long>.InnerNode[] innerNodes = (InnerNode[]) Array.newInstance(InnerNode.class, keys.length);
			long[] keyPostions = keyNode.getChildOrKeyAddr();
			for (int i = 0; i < keys.length; ++i) {
				String key = new String(keys[i], JueConstant.CHARSET);
				innerNodes[i] = node.new InnerNode(key, keyPostions[i]);
			}
			node.setInnerNodes(innerNodes);
		} else {
			// 初始化内部节点和子节点
			BNode<String , Long>.InnerNode[] innerNodes = (InnerNode[]) Array.newInstance(InnerNode.class, keys.length);
			for (int i = 0; i < keys.length; ++i) {
				String key = new String(keys[i], JueConstant.CHARSET);
				innerNodes[i] = node.new InnerNode(key, null);
			}
			node.setInnerNodes(innerNodes);
			
			long[] childPostions = keyNode.getChildOrKeyAddr();
			BNode<String , Long>[] childNodes = (BNode<String , Long>[])Array.newInstance(BNode.class, childPostions.length);
			for (int i = 0; i < childPostions.length; ++i) {
				childNodes[i] = createKeyBNode(childPostions[i], keyTreeMin);
			}
			node.setChildNodes(childNodes);
		}
		
		return node;
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
	 * Put一个Doc对象，覆盖原先数据，如果版本号小于零，那么就忽略该版本要求
	 * @param key 主键
	 * @param docObj 文档对象
	 * @param requireRev 该操作基于的版本号
	 * @return 返回操作成功后，数据的版本号
	 */
	public int putOverWrite(String key, DocObject docObj, int requireRev) {
		writeLock.lock();
		try {
			int rev = 0;
			if (requireRev >= 0) {
				int currentRev = getCurrentRev(key);
				if (currentRev >= 0) {
					if (currentRev != requireRev) {
						throw new RevisionInvalidException();
					}
					rev = currentRev + 1;
				}
			}
			return putImpl(key, docObj, rev);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * Put操作的实现方法
	 * @param key
	 * @param docObj
	 * @param rev 
	 * @return
	 */
	private int putImpl(String key, DocObject docObj, int rev) {
		try {
			long writePos = blockFileChannel.size();
			ByteDynamicArray byteArray = new ByteDynamicArray();
			// 添加ValueRecord
			ValueRecord valueRecord = DocUtils.docObjToValueRecord(false, docObj, rev);
			ByteBuffer vRecBuffer = dropTransfer.valueRecordToByteBuffer(valueRecord);
			byteArray.add(vRecBuffer.array());
			// 添加版本树的修改
			BPlusTree<Integer, Long> revTree = getRevTree(key);
			RevTreeCallBack revTreeCallBack = new RevTreeCallBack(writePos + byteArray.size());
			revTree.put(rev, writePos, revTreeCallBack);
			byteArray.add(revTreeCallBack.getBytes());
			// 添加KeyRecord
			long rootNodeAddr = revTree.getRootNode().getPosition();
			long lastestValueAddr = revTree.getLastLeafNode().getPosition();
			KeyRecord keyRecord = DocUtils.createKeyRecord(false, key, rev, rootNodeAddr, lastestValueAddr);
			ByteBuffer kRecBuffer = dropTransfer.keyRecordToByteBuffer(keyRecord);
			// 该KeyRecord的存储地址
			long keyRecordAddr = writePos + byteArray.size();
			byteArray.add(kRecBuffer.array());
			// 添加Key的树修改
			KeyTreeCallBack keyTreeCallBack = new KeyTreeCallBack(writePos + byteArray.size());
			keyTree.put(key, keyRecordAddr, keyTreeCallBack);
			byteArray.add(keyTreeCallBack.getBytes());
			// 添加文件尾
			FileTail oldFileTail = fileTail;
			if (oldFileTail == null) {
				oldFileTail = new FileTail(0, 0, 0, 0, 0);
			}
			int newEntryCount = oldFileTail.getEntryCount() + 1;
			int newAvgKeyLen = (oldFileTail.getAvgKeyLen() * oldFileTail.getEntryCount() + keyRecord.getKey().length) / newEntryCount;
			int newAvgValueLen = (oldFileTail.getAvgValueLen() * oldFileTail.getEntryCount() + valueRecord.getValue().length) / newEntryCount;
			FileTail tail = new FileTail(oldFileTail.getRevision() + 1, keyTree.getRootNode().getPosition(), newAvgKeyLen, newAvgValueLen, newEntryCount);
			ByteBuffer tailBuffer = dropTransfer.tailToByteBuffer(tail);
			// 文件尾的地址
			long tailAddr = writePos + byteArray.size();
			byteArray.add(tailBuffer.array());
			// 写入文件
			ByteBuffer writeBuffer = ByteBuffer.allocate(byteArray.size());
			writeBuffer.put(byteArray.toByteArray());
			writeBuffer.flip();
			blockFileChannel.write(writeBuffer, writePos);
			// 更新文件头
			final FileHeader oldHeader = fileHeader;
			FileHeader newHeader = new FileHeader(oldHeader.getKeyTreeMin(), oldHeader.getValueRevTreeMin(), oldHeader.getValueCompressed(), oldHeader.getCompressionCodec());
			newHeader.setFileTail(tailAddr);
			ByteBuffer headerBuffer = dropTransfer.headerToByteBuffer(newHeader);
			blockFileChannel.write(headerBuffer, 0);
			// 更新文件头和文件尾
			fileTail = tail;
			fileHeader = newHeader;
			// 清除缓存
			cache.remove(key);
			return rev;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	private BPlusTree<Integer, Long> getRevTree(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 获取key对应的当前版本号，如果该key不存在，则返回-1
	 * @param key
	 * @return key对应的当前版本号，或者key不存在则返回-1
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	private int getCurrentRev(String key) throws IOException, ChecksumException {
		// 从缓存获取当前记录
		CacheObject cacheObj = cache.get(key);
		if (cacheObj != null) {
			return cacheObj.currentRev;
		}
		// 缓存不存在，从索引中获取key信息
		Long recordAddr = keyTree.get(key);
		if (recordAddr != null) {
			KeyRecord record = dropTransfer.readKeyRecord(recordAddr.longValue());
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
