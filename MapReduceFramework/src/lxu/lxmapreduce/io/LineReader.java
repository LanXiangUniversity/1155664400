package lxu.lxmapreduce.io;

import lxu.lxmapreduce.io.format.Text;

import java.io.*;

/**
 * Read lines form inputStream
 * Created by Wei on 11/11/14.
 */

public class LineReader {
	private BufferedReader in;

	public LineReader(String fileName) throws FileNotFoundException {
		try {
			in = new BufferedReader(new FileReader(fileName));
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException("Cannot open the file: " + fileName);
		}

	}

	/**
	 * Read a line into Text
	 * @param str
	 * @return 0 if reach the end of file.
	 * @throws IOException
	 */
	public int readLine(Text str) throws IOException {
		String buffer = null;

		buffer = this.in.readLine();


		if (buffer == null) {
			str.set(null);

			return 0;
		}

		str.set(buffer);

		return buffer.length();
	}

	public int readLine(Text str, int maxLineNum) {
		return 0;
	}

	public void close() throws IOException {
		this.in.close();
	}
}
