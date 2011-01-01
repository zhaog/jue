package com.googlecode.jue.test;

import com.googlecode.jue.bplustree.BPlusTree;

import junit.framework.Assert;
import junit.framework.TestCase;


public class TreeTest extends TestCase{
	private BPlusTree<Integer, String> tree;
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		tree = new BPlusTree<Integer, String>(4);
	}

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		tree = null;
	}
	
	/**
	 * 添加数据
	 */
	public void testTreePut() {
		for (int i = 1; i <= 100; ++i) {
			Assert.assertTrue(tree.put(i, i + ""));
		}
	}
	
	/**
	 * 获取数据
	 */
	public void testTreeGet() {
		for (int i = 1; i <= 100; ++i) {
			tree.put(i, i + "");
		}
		for (int i = 1; i <= 100; ++i) {
			Assert.assertEquals(String.valueOf(i), tree.get(i));
		}
	}
	
}
