package lxu.lxmapreduce.io.format;

import lxu.lxmapreduce.io.RecordReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by Wei on 11/16/14.
 */
public class ReduceReader {
	private HashMap<Text, Iterator<Text>> data;
	private Iterator<Text> keys;
	private Text key;
	private Iterator<Text> value;


	public void initialize(HashMap<Text, Iterator<Text>> input) throws FileNotFoundException {
		// Init input for reduce
		this.data = input;
		this.keys = data.keySet().iterator();
	}

	/**
	 * Get the current value.
	 */
	public Iterator<Text> getCurrentValue() {
		return this.value;
	}
	/**
	 * Get the current key.
	 */
	public Text getCurrentKey() {
		return this.key;
	}

	/**
	 * Read the next key value pair.
	 *
	 * @return
	 */
	public  boolean nextKeyValue() throws IOException {
		if (this.keys.hasNext()) {
			this.key = this.keys.next();
			this.value = this.data.get(this.key);

			return true;
		}

		return false;
	}
	/**
	 * Close the record reader.
	 */
	public void close() throws IOException {

	}
}
