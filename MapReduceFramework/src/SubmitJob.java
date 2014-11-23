import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by Wei on 11/22/14.
 */
public class SubmitJob {
	public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
		String path = null;
		String className = null;

		if (args.length == 0) {
			path = "/Users/parasitew/Documents/testDir/Test.jar";
			className = "TestJob.class";
		} else {
			path = args[0];
			className = args[1] + ".class";
		}

		Class<?> c = null;

		try {
			JarFile jarFile = new JarFile(new File(path));
			URL url = new URL("file:" + path);
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

		Method m = c.getMethod("main", String[].class);
		Object[] objs = new Object[1];
		m.invoke(c.newInstance(), objs);
	}
}
