/**
 * 
 */
package com.googlecode.jue.test;

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
		Jue jue = new Jue("jueTestFile.jue");
//		DocObject docObj = new DocObject();
//		docObj.put("key", true);
//		jue.putOverWrite("test", docObj, -1);
		DocObject obj = jue.get("test", -1);
	}

}
