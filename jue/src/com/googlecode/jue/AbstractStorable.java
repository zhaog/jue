/**
 * 
 */
package com.googlecode.jue;

import java.nio.ByteBuffer;


/**
 * 可存储的接口，对象可以转换成ByteBuffer进行存储
 * @author noah
 *
 */
public abstract class AbstractStorable implements Storable{

	@Override
	public ByteBuffer getByteBuffer() {
		ByteBuffer buffer = ByteBuffer.wrap(toString().getBytes());
		return buffer;
	}
	
}
