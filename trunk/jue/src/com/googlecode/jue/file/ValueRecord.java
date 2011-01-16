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

	private byte flag;
	
	private byte[] value;
	
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
