package lxu.lxdfs.fs;

import lxu.lxdfs.DFSClient;
import lxu.lxdfs.FSDataInputStream;
import lxu.lxdfs.FSDataOutputStream;

import java.net.URI;
import java.nio.file.Path;

/**
 * Created by Wei on 11/1/14.
 */
public class DistributedFileSystem extends FileSystem {
	private Path workingDir;
	private URI uri;
	private DFSClient dfsClient;

	public DistributedFileSystem() {}

	/*
		File operations.
	 */
	public FSDataInputStream open(Path f) {
		return this.dfsClient.open(f);
	}

	public FSDataOutputStream create(Path f) {
		return this.dfsClient.create(f);
	}

	public boolean delete(Path f) {
		this.dfsClient.delete(f);

		return true;
	}

	public boolean close(Path f) {
		this.dfsClient.close(f);

		return true;
	}

	public boolean exists(Path f) {
		this.dfsClient.close(f);

		return true;
	}

	/*
		Setters and Getters
	 */
	public void setWorkingDir(Path workingDir) { this.workingDir = workingDir;}
	public void setUri(URI uri) { this.uri = uri;}

	public Path getWorkingDirectory() { return this.workingDir;}
	public URI getUri() { return this.uri;}

}
