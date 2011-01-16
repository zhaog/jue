/**
 * 
 */
package com.googlecode.jue.file;

/**
 * Valu的记录信息
 * @author noah
 *
 */
public class ValueRecord {

	/**
	 * 标志符，可以标志是否已删除或其他状态
	 */
	private byte flag;
	
	/**
	 * value内容
	 */
	private byte[] value;
	
	/**
	 * 记录的当前版本
	 */
	private int revision;

	public ValueRecord(byte flag, byte[] value, int revision) {
		super();
		this.flag = flag;
		this.value = value;
		this.revision = revision;
	}

	public byte getFlag() {
		return flag;
	}

	public byte[] getValue() {
		return value;
	}

	public int getRevision() {
		return revision;
	}
	
	
}
