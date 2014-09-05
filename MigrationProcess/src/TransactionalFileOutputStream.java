import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class TransactionalFileOutputStream extends OutputStream implements Serializable {
	private FileStatus fileStatus = FileStatus.CLOSED;
	private int position = 0;
	private File file;
	private boolean d1;

	public TransactionalFileOutputStream(String filename, boolean d) {
		file = new File(filename);
	}

	@Override
	public void write(int b) throws IOException {

	}
}
