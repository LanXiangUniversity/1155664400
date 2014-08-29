import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Created by parasitew on 8/28/14.
 */
public class TransactionalFileOutputStream extends OutputStream implements Serializable{
	private FileStatus fileStatus;
	private int position;


	@Override
	public void write(int b) throws IOException {

	}
}
