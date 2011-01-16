/**
 * 
 */
package com.googlecode.jue.file;

/**
 * 文件尾信息
 * @author noah
 *
 */
public class FileTail {
	
	/**
	 * 文件版本号
	 */
	private int revision;
	
	/**
	 * Key的B+树的根节点地址
	 */
	private long rootNode;
	
	/**
	 * 主键key的平均长度
	 */
    private int avgKeyLen;
    
    /**
     * 值Value的平均长度
     */
    private int avgValueLen;
    
    /**
     * K-V 对的数量
     */
    private int entryCount;

	public FileTail() {
		super();
	}

	public FileTail(int revision, long rootNode, int avgKeyLen,
			int avgValueLen, int entryCount) {
		super();
		this.revision = revision;
		this.rootNode = rootNode;
		this.avgKeyLen = avgKeyLen;
		this.avgValueLen = avgValueLen;
		this.entryCount = entryCount;
	}

	public int getRevision() {
		return revision;
	}

	public void setRevision(int revision) {
		this.revision = revision;
	}

	public long getRootNode() {
		return rootNode;
	}

	public void setRootNode(long rootNode) {
		this.rootNode = rootNode;
	}

	public int getAvgKeyLen() {
		return avgKeyLen;
	}

	public void setAvgKeyLen(int avgKeyLen) {
		this.avgKeyLen = avgKeyLen;
	}

	public int getAvgValueLen() {
		return avgValueLen;
	}

	public void setAvgValueLen(int avgValueLen) {
		this.avgValueLen = avgValueLen;
	}

	public int getEntryCount() {
		return entryCount;
	}

	public void setEntryCount(int entryCount) {
		this.entryCount = entryCount;
	}
    
    
}
