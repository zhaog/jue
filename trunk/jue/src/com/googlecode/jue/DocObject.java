/**
 * 
 */
package com.googlecode.jue;

import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 文档对象类
 * @author noah
 *
 */
public class DocObject extends AbstractStorable {
	
	/**
	 * json代理对象
	 */
	private JSONObject jsonObj;
	
	/**
	 * 创建空的文档对象
	 */
	public DocObject() {
		jsonObj = new JSONObject();
	}

	/**
	 * 使用map创建文档对象
	 * @param map
	 */
	public DocObject(Map<String, Object> map) {
		jsonObj = new JSONObject(map);
	}

	/**
	 * 使用字符串创建空的文档对象
	 * @param source
	 * @throws DocException 
	 */
	public DocObject(String source) throws DocException {
		try {
			jsonObj = new JSONObject(source);
		} catch (JSONException e) {
			throw new DocException(e);
		}
	}

	/**
	 * 获取key对应的value对象
	 * @param key
	 * @return
	 * @throws DocException 
	 */
	public Object get(String key) throws DocException {
		try {
			return jsonObj.get(key);
		} catch (JSONException e) {
			throw new DocException(e);
		}
	}
	
	/**
	 * 获取文档对象数组
	 * @param key
	 * @return
	 * @throws DocException
	 */
	public DocArray getDocArray(String key) throws DocException {
		try {
			return (DocArray) jsonObj.get(key);
		} catch (JSONException e) {
			throw new DocException(e);
		}
	}

	/**
	 * 是否包含该键
	 * @param key
	 * @return
	 */
	public boolean contain(String key) {
		return jsonObj.has(key);
	}
	
	/**
	 * 返回键的迭代器
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Iterator<String> keys() {
		return jsonObj.keys();
	}

	/**
	 * 添加或者更新一个键
	 * @param key
	 * @param value
	 * @return
	 * @throws DocException 
	 */
	public DocObject put(String key, Object value) throws DocException {
		try {
			jsonObj.put(key, value);
		} catch (JSONException e) {
			throw new DocException(e);
		}
		return this;
	}
	
	/**
	 * 添加或者更新一个键数组
	 * @param key
	 * @param array
	 * @return
	 * @throws DocException
	 */
	public DocObject put(String key, DocArray array) throws DocException {
		try {
			jsonObj.put(key, array);
		} catch (JSONException e) {
			throw new DocException(e);
		}
		return this;
	}

	/**
	 * 移除一个键
	 * @param key
	 * @return
	 */
	public Object remove(String key) {
		return jsonObj.remove(key);
	}

	@Override
	public String toString() {
		return jsonObj.toString();
	}

}
