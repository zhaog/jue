/**
 * 
 */
package com.googlecode.jue;

import java.util.Collection;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * 文档对象数组
 * @author noah
 *
 */
public class DocArray extends AbstractStorable {
	/**
	 * 文档对象数组代理类
	 */
	private JSONArray jsonArray;
	
	/**
	 * 创建一个空文档数组
	 */
    public DocArray() {
    	jsonArray = new JSONArray();
    }
    
    /**
	 * 使用字符串创建一个文档数组
     * @throws DocException 
	 */
    public DocArray(String source) throws DocException {
    	try {
			jsonArray = new JSONArray(source);
		} catch (JSONException e) {
			throw new DocException(e);
		}
    }
    
    /**
	 * 使用集合创建一个文档数组
	 */
    public DocArray(Collection<?> collection) {
    	jsonArray = new JSONArray(collection);
    }
    
    /**
     * 获取index元素
     * @param index
     * @return
     * @throws DocException 
     */
    public Object get(int index) throws DocException {
    	try {
			return jsonArray.get(index);
		} catch (JSONException e) {
			throw new DocException(e);
		}
    }
    
    /**
     * 获取元素的数量
     * @return
     */
    public int length() {
        return jsonArray.length();
    }
    
    /**
     * 添加数据
     * @param value
     * @return
     */
    public DocArray put(Object value) {
        jsonArray.put(value);
        return this;
    }
    
    /**
     * 添加或者覆盖数据
     * @param value
     * @return
     * @throws DocException 
     */
    public DocArray put(int index, Object value) throws DocException {
        try {
			jsonArray.put(index, value);
		} catch (JSONException e) {
			throw new DocException(e);
		}
        return this;
    }

    /**
     * 移除对应的值
     * @param index
     * @return
     */
    public Object remove(int index) {
    	return jsonArray.remove(index);
    }
    
    @Override
    public String toString() {
    	return jsonArray.toString();
    }

}
