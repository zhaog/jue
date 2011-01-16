/**
 * 
 */
package com.googlecode.jue.file;

/**
 * 文件头信息
 * @author noah
 *
 */
public class FileHeader {
	/**
	 * 文件尾的位置
	 */
	private long fileTail;
	
	/**
	 * key的B+树的最小关键字数
	 */
	private int keyTreeMin;
	
	/**
	 * 主键的类型，0代表String，1代表int，2代表float
	 */
	private byte keyType;
	
	/**
	 * value的版本树的最小关键字数
	 */
	private int valueRevTreeMin;
	
	/**
	 * 版本树的主键的类型，0代表String，1代表int，2代表float
	 */
	private byte valueRevType;
	
	/**
	 * 是否压缩
	 */
	private byte compressed;
	
	/**
	 * 采用的压缩编码
	 */
	private byte compressionCodec;

	public FileHeader() {
		super();
	}

	public long getFileTail() {
		return fileTail;
	}

	public void setFileTail(long fileTail) {
		this.fileTail = fileTail;
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

	public byte getValueRevType() {
		return valueRevType;
	}

	public void setValueRevType(byte valueRevType) {
		this.valueRevType = valueRevType;
	}

	public byte getKeyType() {
		return keyType;
	}

	public void setKeyType(byte keyType) {
		this.keyType = keyType;
	}

	public void setValueRevTreeMin(int valueRevTreeMin) {
		this.valueRevTreeMin = valueRevTreeMin;
	}

	public byte getCompressed() {
		return compressed;
	}

	public void setCompressed(byte compressed) {
		this.compressed = compressed;
	}

	public byte getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(byte compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	
}
