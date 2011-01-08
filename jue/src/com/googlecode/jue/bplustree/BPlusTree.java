package com.googlecode.jue.bplustree;

import java.io.Serializable;

/**
 * @author Noah
 *
 */
public class BPlusTree<K extends Comparable<K>, V extends Serializable> {
	/**
	 * 根节点
	 */
	private BNode<K, V> rootNode;
	
	/**
	 * 第一个叶节点，用于以链式方式遍历所有叶节点
	 */
	private BNode<K, V> firstLeafNode;
	
	/**
	 * 最后一个叶节点，用于以链式方式倒序遍历所有叶节点
	 */
	BNode<K, V> lastLeafNode;

	/**
	 * 树的高度
	 */
	int treeLevel;
	
	/**
	 * 键的总数
	 */
	int keySum;
	
	/**
	 * 节点总数
	 */
	int nodeSum;
	
	/**
	 * 最小关键字树
	 */
	private int m;

	public BPlusTree(int m) {
		super();
		this.m = m;
	}

	/**
	 * 获取根节点
	 * @return
	 */
	public BNode<K, V> getRootNode() {
		if (rootNode == null) {
			rootNode = new BNode<K, V>(this, this.m, true);
			firstLeafNode = rootNode;
			lastLeafNode = rootNode;
			treeLevel = 1;
			nodeSum = 1;
		}
		return rootNode;
	}

	/**
	 * 设置新的根节点
	 * @param rootNode
	 */
	void setRootNode(BNode<K, V> rootNode) {
		this.rootNode = rootNode;
	}

	/**
	 * 返回最小关键字数
	 * @return
	 */
	public int getM() {
		return m;
	}
	
	public int getKeySum() {
		return keySum;
	}

	public int getNodeSum() {
		return nodeSum;
	}

	public int getTreeLevel() {
		return treeLevel;
	}

	public BNode<K, V> getFirstLeafNode() {
		return firstLeafNode;
	}

	public BNode<K, V> getLastLeafNode() {
		return lastLeafNode;
	}

	/**
	 * 插入新键值
	 * @param key
	 * @param value
	 * @return
	 */
	public boolean put(K key, V value) {
		return put(key, value, null);
	}
	
	/**
	 * 插入新键值
	 * @param key
	 * @param value
	 * @param callback
	 * @return
	 */
	public boolean put(K key, V value, TreeCallBack<K, V> callback) {
		return getRootNode().put(key, value, callback);
	}
	
	/**
	 * 查找对应值
	 * @param key
	 * @return
	 */
	public V get(K key) {
		return getRootNode().search(key);
	}
	
	/**
	 * 删除对应的键和值
	 * @param key
	 * @return
	 */
	public boolean delete(K key) {
		return getRootNode().delete(key, null);
	}
	
	/**
	 * 删除对应的键和值
	 * @param key
	 * @return
	 */
	public boolean delete(K key, TreeCallBack<K, V> callback) {
		return getRootNode().delete(key, callback);
	}

	@Override
	public String toString() {
		StringBuilder nodeStr = new StringBuilder();
		getRootNode().printNode(0, nodeStr);
		StringBuilder sb = new StringBuilder();
		sb.append("tree:{keySum:").append(keySum)
			.append(", nodeSum:").append(nodeSum)
			.append(", treeLevel:").append(treeLevel)
			.append("}\n")
			.append(nodeStr);
		return sb.toString();
	}
}
