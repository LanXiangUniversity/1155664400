package processes;

import java.io.*;
import java.util.RandomAccess;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class TransactionalFileInputStream extends InputStream implements Serializable {
	private FileStatus fileStatus = FileStatus.CLOSED;
	private int position = 0;
	private String fileName;

	public TransactionalFileInputStream(String filename) {
		this.fileName = filename;
	}

	@Override
	public int read() throws IOException {
		this.fileStatus = FileStatus.OPEN;
		File file = new File(fileName);

		RandomAccessFile raf = new RandomAccessFile(file, "r");
		raf.seek(position);
		char ch = (char) raf.readByte();
		raf.close();

		this.position++;
		return ch;
	}

	@Override
	public void close() throws IOException {
		super.close();
		this.fileStatus = FileStatus.CLOSED;
		position = 0;
	}
}
