import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

/**
 * Created by parasitew on 8/28/14.
 */
public class TransactionalFileInputStream extends InputStream implements Serializable {
	private FileStatus fileStatus;
	private int position;

	@Override
	public int read() throws IOException {
		return 0;
	}
}
