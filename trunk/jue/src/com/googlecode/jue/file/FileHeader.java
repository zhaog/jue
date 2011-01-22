/**
 * 
 */
package com.googlecode.jue.file;

/**
 * 文件头信息
 * @author noah
 *
 */
public class FileHeader implements ADrop {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2193314360648043655L;
	
	/**
	 * FileHeader的长度，20个字节
	 */
	public static final int HEADER_LENGHT = 20;

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
	 * Value是否压缩
	 */
	private byte valueCompressed;
	
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

	public byte getValueCompressed() {
		return valueCompressed;
	}

	public void setValueCompressed(byte valueCompressed) {
		this.valueCompressed = valueCompressed;
	}

	public byte getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(byte compressionCodec) {
		this.compressionCodec = compressionCodec;
	}

	@Override
	public String toString() {
		return "FileHeader [fileTail=" + fileTail + ", keyTreeMin="
				+ keyTreeMin + ", keyType=" + keyType + ", valueRevTreeMin="
				+ valueRevTreeMin + ", valueRevType=" + valueRevType
				+ ", valueCompressed=" + valueCompressed
				+ ", compressionCodec=" + compressionCodec + "]";
	}

	
}
