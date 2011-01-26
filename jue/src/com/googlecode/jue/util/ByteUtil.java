package com.googlecode.jue.util;

/**
 * 字节工具类
 * 
 * @author noah
 * 
 */
public class ByteUtil {

	/**
	 * int转byte[]
	 * 
	 * @param i
	 * @return
	 */
	public static byte[] int2byte(int i) {
		byte[] result = new byte[4];

		result[0] = (byte) (i & 0xff);
		result[1] = (byte) ((i >> 8) & 0xff);
		result[2] = (byte) ((i >> 16) & 0xff);
		result[3] = (byte) (i >>> 24);
		return result;
	}

	/**
	 * byte[]转int
	 * 
	 * @param b
	 * @return
	 */
	public static int byte2int(byte[] b) {
		int result = (b[0] & 0xff) | ((b[1] << 8) & 0xff00)
				| ((b[2] << 24) >>> 8) | (b[3] << 24);
		return result;
	}

	/**
	 * long转byte[]
	 * 
	 * @param l
	 * @return
	 */
	public static byte[] long2byte(long l) {
		byte[] b = new byte[8];
		b[0] = (byte) (l >> 56);
		b[1] = (byte) (l >> 48);
		b[2] = (byte) (l >> 40);
		b[3] = (byte) (l >> 32);
		b[4] = (byte) (l >> 24);
		b[5] = (byte) (l >> 16);
		b[6] = (byte) (l >> 8);
		b[7] = (byte) (l >> 0);
		return b;
	}

	/**
	 * byte[]转long
	 * @param b
	 * @return
	 */
	public static long byte2long(byte[] b) {
		return ((((long) b[0] & 0xff) << 56) | (((long) b[1] & 0xff) << 48)
				| (((long) b[2] & 0xff) << 40) | (((long) b[3] & 0xff) << 32)
				| (((long) b[4] & 0xff) << 24) | (((long) b[5] & 0xff) << 16)
				| (((long) b[6] & 0xff) << 8) | (((long) b[7] & 0xff) << 0));
	}
}