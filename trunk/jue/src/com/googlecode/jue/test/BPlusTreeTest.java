package com.googlecode.jue.test;

import java.io.Serializable;
import java.util.Random;

import com.googlecode.jue.bplustree.BNode;
import com.googlecode.jue.bplustree.CopyOnWriteBPlusTree;
import com.googlecode.jue.bplustree.TreeCallBack;


public class BPlusTreeTest {

	/**
	* @param args
	 * @throws CloneNotSupportedException 
	*/
	public static void main(String[] args) throws CloneNotSupportedException {
//		BPlusTree<Integer, String> tree = new BPlusTree<Integer, String>(2);
		CopyOnWriteBPlusTree<Integer, String> tree = new CopyOnWriteBPlusTree<Integer, String>(2);
		System.out.println(tree);
		
//		System.out.println("=====put 1:'1'");
//		tree.put(1, "1");
//		System.out.println(tree);
//		
//		System.out.println(">>>>delete 1");
//		tree.delete(1);
//		System.out.println(tree);
		
//		
//		System.out.println("=====put 2:'2'");
//		tree.put(2, "2");
//		System.out.println(tree);
//		
//		System.out.println("=====put 3:'3'");
//		tree.put(3, "3");
//		System.out.println(tree);
		
//		System.out.println("=====search 1");
//		System.out.println(tree.search(1));
//		
		System.out.println("=====put 1~20");
		TreeCallBack<Integer, String> callBack = new TreeCallBack<Integer, String>() {

			@Override
			public void nodeUpdated(BNode<Integer, String> node) {
				System.out.println("nodeUpdated:" + node.toString());
			}

			@Override
			public void nodeSplited(BNode<Integer, String> leftNode,
					BNode<Integer, String> rightNode) {
				System.out.println("nodeSplited:\n" +
									"\tleftNode:" + leftNode.toString() +
									"\n\trightNode:" + rightNode.toString());
			}

			@Override
			public void nodeMerge(BNode<Integer, String> mergedNode) {
				System.out.println("nodeMerge:" + mergedNode.toString());
			}

			@Override
			public void nodeNotChanged(BNode<Integer, String> node) {
				System.out.println("nodeNotChanged:" + node.toString());
			}

			@Override
			public void rootChanged(BNode<Integer, String> rootNode) {
				System.out.println("rootChanged:" + rootNode);
			}

			@Override
			public void nodeMoved(BNode<Integer, String> leftNode,
					BNode<Integer, String> rightNode) {
				System.out.println("nodeMove:\n" +
									"\tleftNode:" + leftNode.toString() +
									"\n\trightNode:" + rightNode.toString());
			}
		};
		for (int i = 1; i <= 100; ++i) {
//			int j = new Random().nextInt(100);
			int j = i;
			System.out.println(">>>>put " + j + ":'" + j + "'");
//			tree.put(j, j + "", callBack);
			tree.put(j, j + "");
			System.out.println(tree);
		}
		System.out.println(tree);
		
//		System.out.println("==========clone");
//		BPlusTree<Integer, String> newTree = tree.clone();
//		System.out.println(newTree);
//		traverseLeaf(tree);
		
//		System.out.println(">>>>delete " + 2);
//		tree.delete(2, callBack);
//		System.out.println(tree);
//		
//		System.out.println(">>>>delete " + 16);
//		tree.delete(16, callBack);
//		System.out.println(tree);
		
		
		System.out.println("=====delete 1~100");
		for (int i = 100; i > 0; --i) {
			int j = new Random().nextInt(100);
			
			System.out.println(">>>>delete " + j);
//			tree.delete(j, callBack);
			tree.delete(j);
			System.out.println(tree);
		}
		
//		System.out.println("=====search 1~10");
//		for (int i = 1; i <= 10; ++i) {
//			System.out.println(">>>>search " + i);
//			System.out.println(tree.search(i));
//		}
		
//		System.out.println(">>>>delete 2");
//		tree.delete(2);
//		System.out.println(tree);
		
//		System.out.println("=====delete 1~100");
//		for (int i = 1; i <= 100; ++i) {
//			System.out.println(">>>>delete " + i);
//			tree.delete(i);
//			System.out.println(tree);
//		}
		
//		Integer[] array = new Integer[]{2, 4, 6, 8, 10};
//		System.out.println(searchKey(array, 5, false));
		
//		System.out.println("=====put 1~100~");
//		for (int i = 1; i <= 100; ++i) {
//			System.out.println(">>>>put " + i + ":'" + i + "'");
//			if (i == 6) {
//				System.out.println("dadf");
//			}
//			tree.put(i, i + "~");
//			System.out.println(tree);
//		}
//		System.out.println(tree.search(6));

//		traverseLeaf(tree);
	}
	
	private static <K extends Comparable<K>, V extends Serializable> void traverseLeaf(CopyOnWriteBPlusTree<K, V> tree) {
		System.out.println("=======顺序遍历");
		BNode<K, V> firstNode = tree.getFirstLeafNode();
		do {
			System.out.println(firstNode);
			firstNode = firstNode.getNextNode();
		} while (firstNode != null);
		
		System.out.println("=======倒序遍历");
		BNode<K, V> lastNode = tree.getLastLeafNode();
		do {
			System.out.println(lastNode);
			lastNode = lastNode.getPrevNode();
		} while (lastNode != null);
	}
	
	private static int searchKey(Integer[] array, Integer key, boolean foundEquel) {
		// 起点
		int low = 0;
		// 终点
		int high = array.length - 1;
		// 中间索引
		int mid = -1;
		// 比较结果
		int cmp = 0;
		while (low <= high) {
			// 中间索引
			mid = (low + high) >>> 1;
			Integer midVal = array[mid];
		   cmp = midVal.compareTo(key);
		   if (cmp < 0) {// 大于中间关键字，查找后半部分
		   	low = mid + 1;
		   } else if (cmp > 0) {// 小于中间关键字，查找前半部分
		   	high = mid - 1;
		   } else {// 查找到
		   	if (foundEquel) {
		   		return mid;
		   	} else {
		   		return mid + 1;
		   	}
		   	
		   }
		}
		// key not found.
		if (cmp > 0) {
			return mid;
		} else {
			return mid + 1;  
		}
		
	}

}

