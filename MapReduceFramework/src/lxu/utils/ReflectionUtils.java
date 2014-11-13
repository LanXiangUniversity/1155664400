package lxu.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by Wei on 11/12/14.
 */
public class ReflectionUtils {
	/**
	 * Load java class from .jar.
	 *
	 * @param className name of class to be loaded
	 * @param jarPath   path of jar file
	 * @return
	 */
	public static Class loadClassFromJar(String className, String jarPath) {
		Class<?> c = null;

		try {
			JarFile jarFile = new JarFile(new File(jarPath));
			URL url = new URL("file:" + jarPath);
			ClassLoader loader = new URLClassLoader(new URL[]{url});
			Enumeration<JarEntry> jarEntries = jarFile.entries();

			while (jarEntries.hasMoreElements()) {
				JarEntry jarEntry = (JarEntry) jarEntries.nextElement();
				String name = jarEntry.getName();

				if (name != null && name.endsWith(className)) {
					//Class<?> c = Thread.currentThread().getContextClassLoader().loadClass(name.replace("/", ".").substring(0,name.length() - 6));
					// Get user defined class.
					c = loader.loadClass(name.replace("/", ".").substring(0, name.length() - 6));
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		return c;
	}

	public static Object loadInstanceFromJar(String className, String jarPath) {
		Class<?> c = loadClassFromJar(className, jarPath);

		Object obj = null;
		try {
			obj = c.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			e.printStackTrace();
		}

		return obj;
	}

	public static <T> T newInstance(String className) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
		Class<T> theClass = (Class<T>) Class.forName(className);

		return theClass.newInstance();
	}
}
