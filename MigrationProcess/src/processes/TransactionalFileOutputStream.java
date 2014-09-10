package processes;

import java.io.*;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class TransactionalFileOutputStream extends OutputStream implements Serializable {
	private FileStatus fileStatus = FileStatus.CLOSED;
	private int position = 0;
	private String fileName;

	public TransactionalFileOutputStream(String filename, boolean d) {
		this.fileName = filename;
	}

	@Override
	public void write(int b) throws IOException {
		File file = new File(this.fileName);

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		raf.seek(this.position);
		raf.write(b);
		raf.close();
		this.position++;
	}

	@Override
	public void close() throws IOException {
		super.close();
		this.fileStatus = FileStatus.CLOSED;
		this.position = 0;
	}
}
