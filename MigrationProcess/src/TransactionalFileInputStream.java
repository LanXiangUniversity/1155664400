import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Created by Tong Wei on 8/28/14.
 */
public class TransactionalFileInputStream extends InputStream implements Serializable {
	private FileStatus fileStatus = FileStatus.CLOSED;
	private int position = 0;
	private File file;

	public TransactionalFileInputStream(String filename) {
		file = new File(filename);

	}

	@Override
	public int read() throws IOException {

		return 0;
	}
}
