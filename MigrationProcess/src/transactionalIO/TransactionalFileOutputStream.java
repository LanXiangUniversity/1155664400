/**
 * TransactionalFileOutputputStream.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 * 
 * Description: Transactional file output. Using a variable *offset* to record
 * 				current writing location. When writing, firstly we seek to the 
 * 				requisite location, then perform the operation, and close the
 * 				file.
 */

package transactionalIO;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.RandomAccessFile;

public class TransactionalFileOutputStream extends OutputStream implements Serializable {
	private static final long serialVersionUID = -3881329126388008331L;
    private long offset;
    private String fileName;

    public TransactionalFileOutputStream(String fileName, boolean append) {
        this.fileName = fileName;
        offset = append ? new File(fileName).length() : 0L;
    }

	@Override
	public void write(int b) throws IOException {
        RandomAccessFile outFile = new RandomAccessFile(fileName, "rws");
        outFile.seek(offset);
        outFile.write(b);
        offset++;
        outFile.close();
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		RandomAccessFile outFile = new RandomAccessFile(fileName, "rws");
		outFile.seek(offset);
		outFile.write(b);
		offset += b.length;
		outFile.close();
	}
}
