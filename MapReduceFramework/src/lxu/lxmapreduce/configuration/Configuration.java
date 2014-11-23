package lxu.lxmapreduce.configuration;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

/**
 * Configuration.java
 * Created by Wei on 11/11/14.
 *
 * The base class of all configuration. It will read the conf file when created.
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = 1L;
    protected HashMap<String, String> entries = null;

    /**
     * Constructor.
     *
     * Read configuration file when created.
     */
	public Configuration() {
        this.entries = new HashMap<String, String>();
        readAllConf("conf");
    }

	public Configuration(Configuration conf) {
		this.entries = conf.entries;
	}

    /**
     * get
     *
     * Given a conf key name, return the string of its value.
     *
     * @param name
     * @return
     */
    public String get(String name) {
        return entries.get(name);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }

        valueString = valueString.toLowerCase();

        if ("true".equals(valueString)) {
            return true;
        } else if ("false".equals(valueString)) {
            return false;
        } else {
            return defaultValue;
        }
    }

    /**
     * getClassByName
     *
     * Given the name of a class, create that class using java reflection
     *
     * @param name
     * @return
     * @throws ClassNotFoundException
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        ClassLoader classLoader;
        {
            classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = Configuration.class.getClassLoader();
            }
        }
		return Class.forName(name, true, classLoader);
	}

    /**
     * getClass
     *
     * Given the class name, create a Class Object of that class
     *
     * @param className
     * @param defaultValue
     * @return
     */
    public Class<?> getClass(String className, Class<?> defaultValue) {
	    String jarName = this.get("mapreduce.jar.name");

	    URL url = null;

	    try {
		    url = new URL("file:" + jarName);
	    } catch (MalformedURLException e) {
		    e.printStackTrace();
	    }
	    URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
	    Class<? extends URLClassLoader> sysclass = URLClassLoader.class;

	    Method method = null;
	    try {
		    method = sysclass.getDeclaredMethod("addURL", URL.class);
	    } catch (NoSuchMethodException e) {
		    e.printStackTrace();
	    }
	    method.setAccessible(true);
	    try {
		    method.invoke(sysloader, url);
	    } catch (IllegalAccessException | InvocationTargetException e) {
		    e.printStackTrace();
	    }


	    String valueString = get(className);
        if (valueString == null) {
            return defaultValue;
        }

        try {
            return getClassByName(valueString);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * getInt
     *
     * Given a conf key name, return its int value.
     *
     * @param name
     * @param defaultValue
     * @return
     */
    public int getInt(String name, int defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }

        return Integer.parseInt(valueString);
    }

    /**
     * getSocketAddr
     *
     * Given a key name, return the string as if it is the IP address.
     *
     * @param name
     * @param defaultAddr
     * @return
     */
    public String getSocketAddr(String name, String defaultAddr) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultAddr;
        }

        return valueString;
    }

    /**
     * getSocketAddrs
     *
     * Given a key name, return a list of IP addresses.
     *
     * @param name
     * @return
     */
    public String[] getSocketAddrs(String name) {
        String valueString = get(name);
        if (valueString == null) {
            return null;
        }

        return valueString.split(",");
    }

    /**
     * set
     *
     * Given a key and its value, record this entry in configuration.
     *
     * @param name
     * @param value
     */
    public void set(String name, String value) {
        this.entries.put(name, value);
    }

    public void setBoolean(String name, boolean value) {
        set(name, Boolean.toString(value));
    }

    public void setClass(String name, Class<?> theClass) {
        set(name, theClass.getName());
    }

    public void setInt(String name, int value) {
        set(name, Integer.toString(value));
    }

    public void setSocketAddr(String name, String addr) {
        set(name, addr);
    }

    public void setSocketAddrs(String name, String[] addrs) {
        String delim = "";
        StringBuilder joinedAddrs = new StringBuilder();
        for (String addr : addrs) {
            joinedAddrs.append(delim).append(addr);
            delim = ",";
        }
        set(name, joinedAddrs.toString());
    }

    /**
     * readAllConf
     *
     * Read configuration from conf file.
     *
     * @param fileName
     */
    public void readAllConf(String fileName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String configLine = null;
            while ((configLine = reader.readLine()) != null) {
                String[] keyValues = configLine.split("=");
                // skip wrong formatted configuration
                if (keyValues.length != 2) {
                    continue;
                }
                String key = keyValues[0];
                String value = keyValues[1];
                entries.put(key, value);
            }
        } catch (FileNotFoundException e) {
            System.err.println("FATAL: Wrong Configuration file!");
            System.exit(-1);
        } catch (IOException e) {
            System.err.println("FATAL: Reading Configuration file wrong!");
            System.err.println(e.getMessage());
            System.exit(-1);
        }
    }
}
