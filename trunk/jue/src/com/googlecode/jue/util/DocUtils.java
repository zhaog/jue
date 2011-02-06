/**
 * 
 */
package com.googlecode.jue.util;

import java.io.UnsupportedEncodingException;

import com.googlecode.jue.JueConstant;
import com.googlecode.jue.doc.DocObject;
import com.googlecode.jue.file.ADrop;
import com.googlecode.jue.file.KeyRecord;
import com.googlecode.jue.file.ValueRecord;

/**
 * 文档对象工具类
 * @author noah
 *
 */
public class DocUtils {
	
	/**
	 * 将doc转换成ValueRecord
	 * @param deleted
	 * @param docObj
	 * @param rev
	 * @return
	 */
	public static ValueRecord docObjToValueRecord(boolean deleted, DocObject docObj, int rev) {
		byte flag = deleted ? ADrop.FALSE_BYTE : ADrop.TRUE_BYTE;
		String json = docObj.toString();
		ValueRecord valueRecord = null;
		try {
			valueRecord = new ValueRecord(flag, json.getBytes(JueConstant.CHARSET), rev);
		} catch (UnsupportedEncodingException e) {
		}
		return valueRecord;
	}
	
	/**
	 * 创建KeyRecord
	 * @param deleted
	 * @param key
	 * @param rev
	 * @param revRootNode
	 * @param lastestValue
	 * @return
	 */
	public static KeyRecord createKeyRecord(boolean deleted, String key, int rev, long revRootNode, long lastestValue) {
		byte flag = deleted ? ADrop.FALSE_BYTE : ADrop.TRUE_BYTE;
		KeyRecord keyRecord = null;
		try {
			keyRecord = new KeyRecord(flag, key.getBytes(JueConstant.CHARSET), revRootNode, rev, lastestValue);
		} catch (UnsupportedEncodingException e) {
		}
		return keyRecord;
	}
}
