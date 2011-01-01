package com.googlecode.jue.test;

import com.googlecode.jue.bplustree.BPlusTree;


public class BPlusTreeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		BPlusTree<Integer, String> tree = new BPlusTree<Integer, String>(4);
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
		System.out.println("=====put 1~100");
		for (int i = 1; i <= 100; ++i) {
			System.out.println(">>>>put " + i + ":'" + i + "'");
			tree.put(i, i + "");
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
		
		System.out.println("=====put 1~100~");
		for (int i = 1; i <= 100; ++i) {
			System.out.println(">>>>put " + i + ":'" + i + "'");
			if (i == 6) {
				System.out.println("dadf");
			}
			tree.put(i, i + "~");
			System.out.println(tree);
		}
//		System.out.println(tree.search(6));
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
