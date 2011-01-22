/**
 * 
 */
package com.googlecode.jue.file;

/**
 * Key的B+树的节点
 * @author noah
 */
public class KeyNode implements ADrop {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1155519276926658836L;

	/**
	 * 是否是叶子节点
	 */
	private byte leaf;
	
	/**
	 * 主键
	 */
	private byte[][] keys;
	
	/**
	 * 子节点或者键的地址
	 */
	private long[] childOrKeyAddr;

	public KeyNode(byte leaf, byte[][] keys, long[] childOrKeyAddr) {
		super();
		this.leaf = leaf;
		this.keys = keys;
		this.childOrKeyAddr = childOrKeyAddr;
	}

	public byte getLeaf() {
		return leaf;
	}

	public byte[][] getKeys() {
		return keys;
	}

	public long[] getChildOrKeyAddr() {
		return childOrKeyAddr;
	}

	
}
