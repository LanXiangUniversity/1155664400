package lxu.lxmapreduce.io;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Created by Wei on 11/11/14.
 */
public class LineWriter {
	private PrintWriter out;

	public LineWriter(String fileName) throws FileNotFoundException {
		try {
			out = new PrintWriter(new FileWriter(fileName));
		} catch (IOException e) {
			throw new FileNotFoundException("Can't find file: " + fileName);
		}
	}

	public void write(String line) {
        //System.out.println("Line Writer write " + line);
		this.out.write(line);
	}

	public void close() {
		this.out.close();
	}
}
