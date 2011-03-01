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
		byte[] docBytes = null;
		ValueRecord valueRecord = null;
		try {
			if (docObj != null) {
				String json = docObj.toString();
				docBytes = json.getBytes(JueConstant.CHARSET);
			} else {
				docBytes = new byte[0];
			}		
			valueRecord = new ValueRecord(flag, docBytes, rev);
		} catch (UnsupportedEncodingException e) {
		}
		return valueRecord;
	}
	
	/**
	 * 创建KeyRecord
	 * @param deleted
	 * @param keyBytes
	 * @param rev
	 * @param revRootNode
	 * @param lastestValue
	 * @return
	 */
	public static KeyRecord createKeyRecord(boolean deleted, byte[] keyBytes, int rev, long revRootNode, long lastestValue) {
		byte flag = deleted ? ADrop.FALSE_BYTE : ADrop.TRUE_BYTE;
		return new KeyRecord(flag, keyBytes, revRootNode, rev, lastestValue);
	}
	
	
}
