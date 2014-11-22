package lxu.lxmapreduce.io;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import lxu.lxdfs.datanode.DataNode;
import lxu.lxdfs.metadata.Block;
import lxu.lxdfs.metadata.DataNodeDescriptor;
import lxu.lxdfs.metadata.LocatedBlock;
import lxu.lxdfs.metadata.LocatedBlocks;
import lxu.lxmapreduce.io.format.LongWritable;
import lxu.lxmapreduce.io.format.Text;
import lxu.lxmapreduce.tmp.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Treats keys as offset in file and value as line.
 * Created by Wei on 11/11/14.
 */
public class LineRecordReader extends RecordReader<LongWritable, Text> {
	private int lineNum = 0;
	private LineReader in;
	private LongWritable key = null;
	private int currentSplit;
	private int maxSplit;
	private Text value = null;
	private List<String> inputFiles;
	private List<LocatedBlock> locatedBlockses;

	public LineRecordReader() {
		/* TODO Init LineReader */
		if (this.key == null) {
			this.key = new LongWritable();
		}

		if (this.value == null) {
			this.value = new Text();
		}
	}

	@Override
	public void initialize(List<String> inputFiles, List<LocatedBlock> locatedBlockses) throws IOException {
		this.currentSplit = 0;
		this.maxSplit = inputFiles.size() - 1;
		this.inputFiles = inputFiles;
		this.locatedBlockses = locatedBlockses;

		getInputSplit();
	}

	@Override
	public Text getCurrentValue() {
		return this.value;
	}

	@Override
	public LongWritable getCurrentKey() {
		return this.key;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		int res = 0;
		boolean res1 = true;

		this.key.set(this.lineNum++);
		res = in.readLine(this.value);
		if (res == 0) {
			res1 = getInputSplit();
		}

		return (res != 0) && res1;
	}

	private boolean getInputSplit() throws IOException {
		if (this.currentSplit < this.maxSplit) {

			// Local replcia ?
			File file  = new File(inputFiles.get(this.currentSplit));
			// If there is no files in the localhost then get from remote data node.
			if (!file.exists()) {
				// TODO: read from remote.
				for (DataNodeDescriptor dataNode : this.locatedBlockses.get(this.currentSplit).getLocations()) {

					if () {
						if (this.currentSplit > 0) this.in.close();
						this.in = new LineReader(inputFiles.get(this.currentSplit));
						this.currentSplit++;
						return true;
					}
				}
			} else {
				if (this.currentSplit > 0) this.in.close();
				this.in = new LineReader(inputFiles.get(this.currentSplit));
				this.currentSplit++;
				return true;
			}

		}

		return false;
	}

	@Override
	public void close() throws IOException {
		if (this.in != null) {
			in.close();
		}
	}
}
