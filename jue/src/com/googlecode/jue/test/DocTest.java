package com.googlecode.jue.test;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.googlecode.jue.doc.DocArray;
import com.googlecode.jue.doc.DocException;
import com.googlecode.jue.doc.DocObject;


public class DocTest extends TestCase{
	public void testDocObject() {
		String json = "{\"b\":2,\"a\":1}";
		String arrayJson = "[{a:1},{b:2}]";
		try {
			DocArray docArray = new DocArray(arrayJson);
			Assert.assertEquals(1, docArray.getDocObject(0).getInt("a"));
			
			DocObject docObj = new DocObject(json);
			docObj.put("array", docArray);
			Assert.assertEquals(1, docObj.getInt("a"));
			
			System.out.println(docArray.toString());
			System.out.println(docObj.toString());
		} catch (DocException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}
