/**
 * 
 */
package com.googlecode.jue;

/**
 * 文件配置信息，只有再创建文件的时候可以修改，读取文件时，只能
 * @author noah
 *
 */
public class FileConfig {
	
	/**
	 * 默认的文件块大小
	 */
	public static final int DEFAULT_BLOCK_SIZE = 4096;

	/**
	 * 默认的树的最小数关键字数
	 */
	public static final int DEFAULT_TREE_MIN = 10;
	
	/**
	 * 文件块大小
	 */
	private int blockSize;
	
	/**
	 * 是否需要缓存文件块
	 */
	private boolean blockCache;
	
	/**
	 * key的B+树的最小关键字数
	 */
	private int keyTreeMin;
	
	/**
	 * value的版本树的最小关键字数
	 */
	private int valueRevTreeMin;
	
	/**
	 * Value是否压缩
	 */
	private boolean valueCompressed;
	
	/**
	 * 采用的压缩编码
	 */
	private CompressionType compressionType;
	
	/**
	 * 使用默认参数创建Config
	 */
	public FileConfig() {
		blockSize = DEFAULT_BLOCK_SIZE;
		blockCache = true;
		keyTreeMin = valueRevTreeMin = DEFAULT_TREE_MIN;
		valueCompressed = false;
	}

	public int getBlockSize() {
		return blockSize;
	}


	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}


	public boolean isBlockCache() {
		return blockCache;
	}


	public void setBlockCache(boolean blockCache) {
		this.blockCache = blockCache;
	}


	public int getKeyTreeMin() {
		return keyTreeMin;
	}


	public void setKeyTreeMin(int keyTreeMin) {
		this.keyTreeMin = keyTreeMin;
	}


	public int getValueRevTreeMin() {
		return valueRevTreeMin;
	}


	public void setValueRevTreeMin(int valueRevTreeMin) {
		this.valueRevTreeMin = valueRevTreeMin;
	}


	public boolean isValueCompressed() {
		return valueCompressed;
	}


	public void setValueCompressed(boolean valueCompressed) {
		this.valueCompressed = valueCompressed;
	}


	public CompressionType getCompressionType() {
		return compressionType;
	}


	public void setCompressionType(CompressionType compressionType) {
		this.compressionType = compressionType;
	}


	public enum CompressionType {
		
		/**
		 * 不使用压缩方式
		 */
		NOT_COMPRESSED,
		
		/**
		 * gzip压缩方式
		 */
		GZIP,
		
		/**
		 * LZW压缩方式，速度较慢，但是压缩比很大
		 */
		LZW,

		/**
		 * zlib压缩方式
		 */
		ZLIB,
		
		/**
		 * 速度很快，但是压缩比很小
		 */
		QUICKLZ
		
	}
}
