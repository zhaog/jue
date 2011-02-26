/**
 * 
 */
package com.googlecode.jue.test;

import com.googlecode.jue.FileConfig;
import com.googlecode.jue.Jue;
import com.googlecode.jue.doc.DocObject;

/**
 * @author noah
 *
 */
public class JueTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FileConfig config = new FileConfig();
		config.setKeyTreeMin(1);
		config.setValueRevTreeMin(1);
//		Jue jue = new Jue("jueTestFile.jue", config);
		Jue jue = new Jue("jueTestFile.jue");
//		testMerge(jue);
		testGet(jue);
//		for (int i = 0; i < 3; ++i) {
//			DocObject docObj = new DocObject();
//			docObj.put("key" + i, true);
//			jue.put("test", docObj, -1);
////			DocObject obj = jue.get("test" + i, -1);
////			System.out.println(obj);
//		}
//		DocObject obj = jue.get("test", 0);
//		System.out.println(obj);
//		DocObject obj = jue.get("test", 1);
//		int rev = jue.remove("test");
//		System.out.println(rev);
	}

	private static void testGet(Jue jue) {
		DocObject obj = jue.get("test", -1);
		System.out.println(obj);
	}

	private static void testMerge(Jue jue) {
		DocObject docObj = new DocObject();
		docObj.put("nkey", true);
		jue.put("test", docObj, -1, true);
	}

}
