/**
 * 
 */
package com.googlecode.jue.file;

/**
 * Value值的版本树节点
 * @author noah
 *
 */
public class ValueRevNode implements ADrop {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6060521308578833028L;

	/**
	 * 叶子节点
	 */
	public static final byte LEAF = 0x1;
	
	/**
	 * 非叶节点
	 */
	public static final byte NOTLEAF = 0x0;
	
	/**
	 * 是否是叶子节点
	 */
	private byte leaf;
	
	/**
	 * 主键(版本号)
	 */
	private int[] revisions;
	
	/**
	 * 子节点或者键的地址
	 */
	private long[] childOrKeyAddr;

	public ValueRevNode(byte leaf, int[] revisions, long[] childOrKeyAddr) {
		super();
		this.leaf = leaf;
		this.revisions = revisions;
		this.childOrKeyAddr = childOrKeyAddr;
	}

	public byte getLeaf() {
		return leaf;
	}


	public int[] getRevisions() {
		return revisions;
	}

	public long[] getChildOrKeyAddr() {
		return childOrKeyAddr;
	}
	
	
}
