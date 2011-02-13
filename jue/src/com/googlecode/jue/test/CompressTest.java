/**
 * 
 */
package com.googlecode.jue.test;

import java.io.IOException;

import com.googlecode.jue.compression.DataCompress;
import com.googlecode.jue.compression.gzip.GZipDataCompress;
import com.googlecode.jue.compression.lzw.LZWDataCompress;
import com.googlecode.jue.compression.quicklz.QuickLZDataCompress;
import com.googlecode.jue.compression.zlib.ZlibDataCompress;

/**
 * @author noah
 *
 */
public class CompressTest {

	private static final int count = 10;
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws Exception {
//		String s = "[{" +
//						"prevhref:'EditorFiles/Posts/1/80.html'," +
//						"prevtitle:'系统升级完毕'," +
//						"prevEntity:'1'," +
//						"nexthref:'EditorFiles/Posts/1/78.html'," +
//						"nexttitle:'『摄影教程』人像摄影中的13个拍摄灵感'," +
//						"nextEntity:'1'," +
//						"hit:'30'," +
//						"commentlist:[{" +
//							"CommentIndex:173," +
//							"UserName:'ja0963'," +
//							"CommentDate:'2007-12-17 22:27:44'," +
//							"IsRegUser:False," +
//							"Comment:'not bad'}]" +
//						"}]";
//		byte[] data = s.getBytes("UTF-8");
		
		int len = 1 << 16;
		String s = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; ++i) {
			sb.append(s.charAt(i % s.length()));
		}
		byte[] data = sb.toString().getBytes("UTF-8");
		
//		System.out.println("data:" + s);
		System.out.println("size:" + data.length);
		
		LZWDataCompress lzw = new LZWDataCompress();
		doCompress(lzw, data);

		GZipDataCompress gzip = new GZipDataCompress();
		doCompress(gzip, data);
		
		ZlibDataCompress zip = new ZlibDataCompress();
		doCompress(zip, data);
		
		QuickLZDataCompress quickLZ = new QuickLZDataCompress();
		doCompress(quickLZ, data);
		
		
		doDecompress(lzw, data);
		doDecompress(gzip, data);
		doDecompress(zip, data);
		doDecompress(quickLZ, data);
	}

	
	private static void doDecompress(DataCompress compress, byte[] data) throws Exception {
		System.out.println("decompress class:" + compress.getClass().getName());
		byte[] b = compress.compress(data);
		// 预热
		for (int i = 0; i < count; ++i) {
			compress.decompress(b);
		}
		System.out.println("decompress start======");
		long startTime = System.currentTimeMillis();
		byte[] s = null;
		for (int i = 0; i < count; ++i) {
			s = compress.decompress(b);
		}
		System.out.println("escaped:" + (System.currentTimeMillis() - startTime));
//		System.out.println("decompressed data:" + s);
		System.out.println("decompress end======\n");		
	}


	private static void doCompress(DataCompress compress, byte[] data) throws Exception {
		System.out.println("compress class:" + compress.getClass().getName());
		// 预热
		for (int i = 0; i < count; ++i) {
			compress.compress(data);
		}
		
		System.out.println("compress start======");
		long startTime = System.currentTimeMillis();
		byte[] b = null;
		for (int i = 0; i < count; ++i) {
			b = compress.compress(data);
		}
		System.out.println("escaped:" + (System.currentTimeMillis() - startTime));
		System.out.println("compressed size:" + b.length);
		System.out.println("compress end======\n");
		
	}
}
