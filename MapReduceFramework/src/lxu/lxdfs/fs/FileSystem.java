package lxu.lxdfs.fs;

import com.lxdfs.conf.Configuration;
import lxu.lxdfs.FSDataInputStream;
import lxu.lxdfs.FSDataOutputStream;

import java.net.URI;
import java.nio.file.Path;

/**
 * Created by Wei on 11/1/14.
 */
public abstract class FileSystem {

	private static final int CACHE = 0;

	// return DistributedFileSystem in default
	// Gets Name Node information using URI passed
	// Create a DFS Client object

	/*
		Abstract methods
	 */
	public abstract FSDataOutputStream create(Path path);

	public abstract FSDataInputStream open(Path path);

	/*

	 */
	public FileSystem get(URI uri) {
		/**
		 *
		 */
		return new DistributedFileSystem();
	};
}
