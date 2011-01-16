/**
 * 
 */
package com.googlecode.jue.file;

import java.util.zip.Adler32;
import java.util.zip.Checksum;


/**
 * Adler32 算法的校验码生成器
 * @see <a>java.util.zip.Adler32</a>
 * @author noah
 *
 */
public class Adler32ChecksumGenerator implements ChecksumGenerator {
	
	@Override
	public Checksum createChecksum() {
		return new Adler32();
	}

}
