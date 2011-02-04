/**
 * 
 */
package com.googlecode.jue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.googlecode.jue.file.ADrop;
import com.googlecode.jue.file.BlockFileChannel;
import com.googlecode.jue.file.CRC32ChecksumGenerator;
import com.googlecode.jue.file.DropTransfer;
import com.googlecode.jue.file.FileHeader;

/**
 * Jue的主类，创建文件，操作文件等都是通过这个类来执行的。
 * @author noah
 *
 */
public class Jue {

	private DropTransfer dropTransfer;
	
	private BlockFileChannel blockFileChannel;
	
	public Jue(String filePath) {
		this(filePath, new FileConfig());
	}
	
	public Jue(String filePath, FileConfig config) {
		File file = new File(filePath);
		if (!file.exists()) {
			blockFileChannel = new BlockFileChannel(file, config.getBlockSize(), config.isBlockCache(), new CRC32ChecksumGenerator());
			dropTransfer = new DropTransfer(blockFileChannel);
			FileHeader header = new FileHeader();
			header.setFileTail(0x0);
			header.setKeyTreeMin(config.getKeyTreeMin());
			byte valueCompressed = ADrop.FALSE_BYTE;
			byte compressionCodec = (byte) FileConfig.CompressionType.NOT_COMPRESSED.ordinal();
			if (config.isValueCompressed()) {
				valueCompressed = ADrop.TRUE_BYTE;
				compressionCodec = (byte) config.getCompressionType().ordinal();
			}
			header.setValueCompressed(valueCompressed);
			header.setCompressionCodec(compressionCodec);
			header.setValueRevTreeMin(config.getValueRevTreeMin());
			ByteBuffer buffer = dropTransfer.headerToByteBuffer(header);
			try {
				blockFileChannel.write(buffer, 0);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
