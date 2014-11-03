package lxu.fs;

import java.io.File;
import java.nio.file.Path;

/**
 * Mange File System Namespce for Name Node.
 * Created by Wei on 11/2/14.
 */
public class FSNameSystem {
	public FSNameSystem() {
	}

	/**
	 * Create directory in the namenode.
	 * @param path
	 * @return Succeed or not.
	 */
	public boolean mkdirs(Path path) {
		File file = path.toFile();

		if (file.exists()) {
			return false;
		}

		if (!file.mkdirs()) {
			return false;
		}

		return false;
	}

	public boolean open(Path path) {

	}

	public boolean close(Path path) {

	}

	public boolean exists(Path path) {

	}

	public boolean delete(Path path ) {

	}

	public void allocateBlocks(Path path) {

	}


}
