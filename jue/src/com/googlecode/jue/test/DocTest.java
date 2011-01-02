package com.googlecode.jue.test;

import org.junit.Assert;

import com.googlecode.jue.DocArray;
import com.googlecode.jue.DocException;
import com.googlecode.jue.DocObject;

import junit.framework.TestCase;


public class DocTest extends TestCase{
	public void testDocObject() {
		String json = "{\"b\":2,\"a\":1}";
		String arrayJson = "[{a:1},{b:2}]";
		try {
			DocArray docArray = new DocArray(arrayJson);
//			Assert.assertEquals(arrayJson, docArray.toString());
			
			DocObject docObj = new DocObject(json);
			docObj.put("array", docArray);
//			Assert.assertEquals(json, docObj.toString());
			
			System.out.println(docArray.toString());
			System.out.println(docObj.toString());
		} catch (DocException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

}
