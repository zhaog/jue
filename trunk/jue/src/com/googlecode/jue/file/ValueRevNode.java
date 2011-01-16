/**
 * 
 */
package com.googlecode.jue.file;

/**
 * Value值的版本树节点
 * @author noah
 *
 */
public class ValueRevNode {

	/**
	 * 是否是叶子节点
	 */
	private byte leaf;
	
	/**
	 * 主键(版本号)
	 */
	private int[] keys;
	
	/**
	 * 子节点或者键的地址
	 */
	private long[] childOrKeyAddr;

	public ValueRevNode(byte leaf, int[] keys, long[] childOrKeyAddr) {
		super();
		this.leaf = leaf;
		this.keys = keys;
		this.childOrKeyAddr = childOrKeyAddr;
	}

	public byte getLeaf() {
		return leaf;
	}

	public int[] getKeys() {
		return keys;
	}

	public long[] getChildOrKeyAddr() {
		return childOrKeyAddr;
	}
	
	
}
