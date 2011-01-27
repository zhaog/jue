/**
 * 
 */
package com.googlecode.jue.file;

/**
 * Key的信息
 * @author noah
 *
 */
public class KeyRecord implements ADrop {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1481819593702669747L;

	/**
	 * 标志符，可以标志是否已删除或其他状态
	 */
	private byte flag;
	
	/**
	 * key的内容，最长64KB
	 */
	private byte[] key;
	
	/**
	 * Value的版本树的根节点
	 */
	private long revRootNode;
	
	/**
	 * 当前Key的版本
	 */
	private int revision;
	
	/**
	 * 最新版本的Value记录地址
	 */
	private long lastestValue;

	public KeyRecord(byte flag, byte[] key, long revRootNode, int revision, long lastestValue) {
		super();
		this.flag = flag;
		this.key = key;
		this.revRootNode = revRootNode;
		this.revision = revision;
		this.lastestValue = lastestValue;
	}

	public byte getFlag() {
		return flag;
	}

	public byte[] getKey() {
		return key;
	}

	public long getRevRootNode() {
		return revRootNode;
	}

	public int getRevision() {
		return revision;
	}

	public long getLastestValue() {
		return lastestValue;
	}
	
	
}
