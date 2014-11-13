package lxu.lxmapreduce.tmp;

import java.util.HashMap;

/**
 * Created by Wei on 11/11/14.
 */
public class Configuration {
	protected HashMap<String, Object> entries = null;
	private ClassLoader classLoader;
	{
		classLoader = Thread.currentThread().getContextClassLoader();
		if (classLoader == null) {
			classLoader = Configuration.class.getClassLoader();
		}
	}

	public Configuration(Configuration conf) {
		this.entries = conf.entries;
	}

	public Class<?> getClassByName(String name) throws ClassNotFoundException {
		return Class.forName(name, true, classLoader);
	}

	public Class<?> getClass(String className) {
		/* TODO getclass */

		return null;
	}
}
