/**
 * TransactionalFileInputStream.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: Transactional file input. Using a variable *offset* to record
 * 				current reading location. When reading, firstly we seek to the 
 * 				requisite location, then perform the operation, and close the
 * 				file.
 */

package transactionalIO;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class TransactionalFileInputStream extends InputStream
		implements Serializable {

	private static final long serialVersionUID = -3881329126388008331L;
	private long offset;
	private String fileName;

	public TransactionalFileInputStream() {
	}

	public TransactionalFileInputStream(String fileName) {
		this.fileName = fileName;
		offset = 0;
	}

	@Override
	public int read() throws IOException {
		RandomAccessFile inFile = new RandomAccessFile(fileName, "rws");
		inFile.seek(offset);
		int readByte = inFile.read();
		if (readByte != -1)
			offset++;
		inFile.close();
		return readByte;
	}

	@Override
	public int read(byte[] b) throws IOException {
		RandomAccessFile inFile = new RandomAccessFile(fileName, "rws");
		inFile.seek(offset);
		int num = inFile.read(b);
		inFile.close();
		if (num != -1) {
			offset += num;
		}
		return num;

	}
}
