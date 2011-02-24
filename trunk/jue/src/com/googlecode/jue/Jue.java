/**
 * 
 */
package com.googlecode.jue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.googlecode.jue.bplustree.BNode;
import com.googlecode.jue.bplustree.BPlusTree;
import com.googlecode.jue.bplustree.CopyOnWriteBPlusTree;
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
import com.googlecode.jue.file.ValueRevNode;
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
	 * 最大Key长度
	 */
	public static final int MAX_KEY_LENGTH = 1 << 16;
	
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
		try {
			int keyTreeMin = 0;
			if (!exist) {
				fileHeader = createEmptyFile(file, config);
			} else {
				fileHeader = readHeader(file, config);
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
	 * 直接从文件中读取文件头信息
	 * @param file
	 * @param config 
	 * @return
	 * @throws IOException
	 */
	private FileHeader readHeader(File file, FileConfig config) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(FileHeader.HEADER_LENGHT);
		FileChannel readChannel = null;
		try {
			readChannel = new RandomAccessFile(file, "r").getChannel();
			readChannel.read(buffer, 0);
		} finally {
			readChannel.close();
		}
		buffer.flip();
		long fileTail = buffer.getLong();
		int keyTreeMin = buffer.getInt();
		int valueRevTreeMin = buffer.getInt();
		byte valueCompressed = buffer.get();
		byte compressionCodec = buffer.get();
		int blockSize = buffer.getInt();
		
		FileHeader header = new FileHeader();
		header.setFileTail(fileTail);
		header.setKeyTreeMin(keyTreeMin);
		header.setValueRevTreeMin(valueRevTreeMin);
		header.setValueCompressed(valueCompressed);
		header.setCompressionCodec(compressionCodec);
		header.setBlockSize(blockSize);
		
		blockFileChannel = new BlockFileChannel(file, header.getBlockSize(), config.isBlockCache(), new CRC32ChecksumGenerator());
		dropTransfer = new DropTransfer(blockFileChannel);
		
		return header;
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
	private BNode<String, Long> createKeyBNode(long nodePosition, int keyTreeMin) throws IOException, ChecksumException {
		KeyNode keyNode = dropTransfer.readKeyNode(nodePosition);
		boolean isLeaf = keyNode.getLeaf() == KeyNode.TRUE_BYTE;
		BNode<String, Long> node = new BNode<String, Long>(null, keyTreeMin, isLeaf);
		node.setPosition(nodePosition);
		byte[][] keys = keyNode.getKeys();
		node.setCount(keys.length);
		
		if (isLeaf) {
			// 初始化内部节点
			long[] keyPostions = keyNode.getChildOrKeyPos();
			for (int i = 0; i < keys.length; ++i) {
				String key = new String(keys[i], JueConstant.CHARSET);
				BNode<String , Long>.InnerNode innerNode = node.new InnerNode(key, keyPostions[i]);
				node.setInnerNode(i, innerNode);
			}
		} else {
			// 初始化内部节点和子节点
			for (int i = 0; i < keys.length; ++i) {
				String key = new String(keys[i], JueConstant.CHARSET);
				BNode<String , Long>.InnerNode innerNode = node.new InnerNode(key, null);
				node.setInnerNode(i, innerNode);
			}			
			long[] childPostions = keyNode.getChildOrKeyPos();
			for (int i = 0; i < childPostions.length; ++i) {
				BNode<String , Long> childNode = createKeyBNode(childPostions[i], keyTreeMin);
				node.setChildNode(i, childNode);
			}
		}
		return node;
	}

	/**
	 * 创建文件，并添加文件头
	 * @param file 
	 * @param config
	 * @return 
	 * @throws IOException 
	 */
	private FileHeader createEmptyFile(File file, FileConfig config) throws IOException {
		blockFileChannel = new BlockFileChannel(file, config.getBlockSize(), config.isBlockCache(), new CRC32ChecksumGenerator());
		dropTransfer = new DropTransfer(blockFileChannel);
		
		FileHeader header = new FileHeader();
		header.setFileTail(-1);
		header.setKeyTreeMin(config.getKeyTreeMin());
		byte valueCompressed = ADrop.FALSE_BYTE;
		byte compressionCodec = FileConfig.NOT_COMPRESSED;
		if (config.isValueCompressed()) {
			valueCompressed = ADrop.TRUE_BYTE;
			compressionCodec = (byte) config.getCompressionType();
		}
		header.setValueCompressed(valueCompressed);
		header.setCompressionCodec(compressionCodec);
		header.setValueRevTreeMin(config.getValueRevTreeMin());
		header.setBlockSize(config.getBlockSize());
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
		try {
			byte[] keyBytes = key.getBytes(JueConstant.CHARSET);			
			if (keyBytes.length > MAX_KEY_LENGTH) {
				throw new IllegalArgumentException("key length must less than 64KB, actul:" + keyBytes.length);
			}
			writeLock.lock();
			int rev = 0;
			int currentRev = getCurrentRev(key);
			if (requireRev >= 0) {
				if (currentRev != requireRev) {
					throw new RevisionInvalidException();
				}
			}
			rev = currentRev + 1;
			return putImpl(key, keyBytes, docObj, false, rev);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			writeLock.unlock();
		}
	}

	/**
	 * Put操作的实现方法
	 * @param key
	 * @param keyBytes 
	 * @param docObj
	 * @param rev 
	 * @return
	 */
	private int putImpl(String key, byte[] keyBytes, DocObject docObj, boolean deleted, int rev) {
		try {
			long writePos = blockFileChannel.size();
			ByteDynamicArray byteArray = new ByteDynamicArray();
			// 添加ValueRecord
			ValueRecord valueRecord = DocUtils.docObjToValueRecord(deleted, docObj, rev);
			ByteBuffer vRecBuffer = dropTransfer.valueRecordToByteBuffer(valueRecord);
			// ValueRec的写入位置
			long valuePos = writePos;
			byteArray.add(vRecBuffer.array());
			// 添加版本树的修改
			BPlusTree<Integer, Long> revTree = getRevTree(key);
			RevTreeCallBack revTreeCallBack = new RevTreeCallBack(writePos + byteArray.size());
			revTree.put(rev, writePos, revTreeCallBack);
			byteArray.add(revTreeCallBack.getBytes());
			// 添加KeyRecord
			long rootNodePos = revTree.getRootNode().getPosition();
			long lastestValuePos = valuePos;
			KeyRecord keyRecord = DocUtils.createKeyRecord(deleted, keyBytes, rev, rootNodePos, lastestValuePos);
			ByteBuffer kRecBuffer = dropTransfer.keyRecordToByteBuffer(keyRecord);
			// 该KeyRecord的存储地址
			long keyRecordPos = writePos + byteArray.size();
			byteArray.add(kRecBuffer.array());
			// 添加Key的树修改
			KeyTreeCallBack keyTreeCallBack = new KeyTreeCallBack(writePos + byteArray.size());
			keyTree.put(key, keyRecordPos, keyTreeCallBack);
			byteArray.add(keyTreeCallBack.getBytes());
			// 添加文件尾
			FileTail oldFileTail = fileTail;
			if (oldFileTail == null) {
				oldFileTail = new FileTail(0, 0, 0, 0, 0);
			}
			long newEntryCount = oldFileTail.getEntryCount() + 1;
			int newAvgKeyLen = (int) ((oldFileTail.getAvgKeyLen() * oldFileTail.getEntryCount() + keyRecord.getKey().length) / newEntryCount);
			int newAvgValueLen = (int) ((oldFileTail.getAvgValueLen() * oldFileTail.getEntryCount() + valueRecord.getValue().length) / newEntryCount);
			FileTail tail = new FileTail(oldFileTail.getRevision() + 1, keyTree.getRootNode().getPosition(), newAvgKeyLen, newAvgValueLen, newEntryCount);
			ByteBuffer tailBuffer = dropTransfer.tailToByteBuffer(tail);
			// 文件尾的地址
			long tailPos = writePos + byteArray.size();
			byteArray.add(tailBuffer.array());
			// 写入文件
			ByteBuffer writeBuffer = ByteBuffer.allocate(byteArray.size());
			writeBuffer.put(byteArray.toByteArray());
			writeBuffer.flip();
			blockFileChannel.write(writeBuffer, writePos);
			// 更新文件头
			final FileHeader oldHeader = fileHeader;
			FileHeader newHeader = new FileHeader(oldHeader.getKeyTreeMin(), oldHeader.getValueRevTreeMin(), 
													oldHeader.getValueCompressed(), oldHeader.getCompressionCodec(), 
													oldHeader.getBlockSize());
			newHeader.setFileTail(tailPos);
			ByteBuffer headerBuffer = dropTransfer.headerToByteBuffer(newHeader);
			blockFileChannel.write(headerBuffer, 0);
			// 更新文件头和文件尾
			fileTail = tail;
			fileHeader = newHeader;
			// 清除缓存
			cache.remove(key);
			return rev;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
	}

	/**
	 * 获取Value版本树
	 * @param key
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	private BPlusTree<Integer, Long> getRevTree(String key) throws IOException, ChecksumException {
		KeyRecord record = getKeyRecord(key);
		if (record != null) {
			long revRootNodePos = record.getRevRootNode();
			BPlusTree<Integer, Long> revTree = new BPlusTree<Integer, Long>(fileHeader.getValueRevTreeMin());
			BNode<Integer, Long> rootNode = createValueRevNode(revRootNodePos, fileHeader.getValueRevTreeMin());
			revTree.updateNewTree(revTree, rootNode);
			return revTree;
		}
		// 不存在该版本树
		return new BPlusTree<Integer, Long>(fileHeader.getValueRevTreeMin());
	}

	/**
	 * 读取文件，创建节点以及遍历创建子节点
	 * @param nodePos
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	private BNode<Integer, Long> createValueRevNode(long nodePos, int revTreeMin) throws IOException, ChecksumException {
		ValueRevNode valueRevNode = dropTransfer.readValueRevNode(nodePos);
		boolean isLeaf = valueRevNode.getLeaf() == ValueRevNode.TRUE_BYTE;
		BNode<Integer, Long> node = new BNode<Integer, Long>(null, revTreeMin, isLeaf);
		node.setPosition(nodePos);
		int[] revisions = valueRevNode.getRevisions();
		node.setCount(revisions.length);
		
		if (isLeaf) {
			// 初始化内部节点
			long[] keyPostions = valueRevNode.getChildOrKeyPos();
			for (int i = 0; i < revisions.length; ++i) {
				BNode<Integer, Long>.InnerNode innerNode = node.new InnerNode(revisions[i], keyPostions[i]);
				node.setInnerNode(i, innerNode);
			}
		} else {
			// 初始化内部节点和子节点
			for (int i = 0; i < revisions.length; ++i) {
				BNode<Integer, Long>.InnerNode innerNode = node.new InnerNode(revisions[i], null);
				node.setInnerNode(i, innerNode);
			}
			long[] childPostions = valueRevNode.getChildOrKeyPos();
			for (int i = 0; i < childPostions.length; ++i) {
				BNode<Integer , Long> childNode = createValueRevNode(childPostions[i], revTreeMin);
				node.setChildNode(i, childNode);
			}
		}
		return node;
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
		KeyRecord record = getKeyRecord(key);
		if (record != null) {
			return record.getRevision();
		}
		// 该key不存在，返回-1
		return -1;
	}
	
	/**
	 * 获取KeyRecord
	 * @param key
	 * @return
	 * @throws ChecksumException 
	 * @throws IOException 
	 */
	private KeyRecord getKeyRecord(String key) throws IOException, ChecksumException {
		Long recordAddr = keyTree.get(key);
		if (recordAddr != null) {
			KeyRecord record = dropTransfer.readKeyRecord(recordAddr.longValue());
			return record;
		}
		return null;
	}
	
	/**
	 * 获取文档对象
	 * @param key
	 * @param requireRev
	 * @return
	 */
	public DocObject get(String key, int requireRev) {
		CacheObject cacheObj = cache.get(key);
		// 缓存中存在
		if (cacheObj != null) {
			if (requireRev >= 0) {
				if (cacheObj.currentRev == requireRev) {// 版本符合
					return cacheObj.docObj;
				}
			} else {
				return cacheObj.docObj;
			}
		}
		// 从文件中读取
		try {
			KeyRecord keyRecord = getKeyRecord(key);
			if (keyRecord == null) {
				return null;
			}
			// value记录的地址
			long valueRecordPos = 0;
			// 是否是最新版本的数据
			boolean isLastValue = false;
			int lastRevision = -1;
			if (requireRev >= 0) {
				int currentRevision = keyRecord.getRevision();
				if (currentRevision == requireRev) {// 版本为最新数据，读取最新的数据
					valueRecordPos = keyRecord.getLastestValue();
					isLastValue = true;
					lastRevision = currentRevision;
				} else {// 查找其他版本数据
					if (requireRev > currentRevision) {// 版本超过最新版本，数据不存在
						return null;
					}
					BPlusTree<Integer, Long> revTree = getRevTree(key);
					Long valuePos = revTree.get(requireRev);
					if (valuePos == null) {// 该版本数据可能已经被删除
						return null;
					}
					valueRecordPos = valuePos.longValue();
				}
			} else {
				valueRecordPos = keyRecord.getLastestValue();
				isLastValue = true;
				lastRevision = keyRecord.getRevision();
			}
			DocObject docObj = null;
			if (isLastValue) {// 最新数据，所以直接通过KeyRecord判断当前数据状态
				if (!keyRecord.isDeleted()) {
					docObj = readDocObj(valueRecordPos);
				}
				// 最新版本的数据，放入缓存
				cacheObj = new CacheObject(lastRevision, docObj);
				cache.put(key, cacheObj);
			} else {
				docObj = readDocObj(valueRecordPos);
			}
			return docObj;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * 读取文档对象
	 * @param valuePos
	 * @return
	 * @throws IOException
	 * @throws ChecksumException
	 */
	private DocObject readDocObj(long valuePos) throws IOException, ChecksumException {
		// 从文件读取ValueRecord
		ValueRecord valueRecord = dropTransfer.readValueRecord(valuePos);
		if (!valueRecord.isDeleted()) {// 数据存在
			byte[] values = valueRecord.getValue();
			String valueStr = new String(values, JueConstant.CHARSET);
			DocObject docObj = new DocObject(valueStr);
			return docObj;
		}
		return null;
	}
	/**
	 * 删除key对应文档对象
	 * @param key
	 * @return
	 */
	public int remove(String key) {
		try {
			byte[] keyBytes = key.getBytes(JueConstant.CHARSET);			
			if (keyBytes.length > MAX_KEY_LENGTH) {
				throw new IllegalArgumentException("key length must less than 64KB, actul:" + keyBytes.length);
			}
			writeLock.lock();
			int currentRev = getCurrentRev(key);
			return putImpl(key, keyBytes, null, true, currentRev + 1);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			writeLock.unlock();
		}		
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

		public CacheObject(int currentRev, DocObject docObj) {
			super();
			this.currentRev = currentRev;
			this.docObj = docObj;
		}
		
		
	}
}
