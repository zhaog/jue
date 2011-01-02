/**
 * 
 */
package com.googlecode.jue;

import java.nio.ByteBuffer;

/**
 * 可存储的接口，对象可以转换成ByteBuffer进行存储
 * @author noah
 */
public interface Storable {
	/**
	 * 转换成ByteBuffer
	 * @return
	 */
	ByteBuffer getByteBuffer();
}
