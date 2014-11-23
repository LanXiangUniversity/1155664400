package lxu.lxmapreduce.configuration;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;

/**
 * Created by Wei on 11/11/14.
 */
public class Configuration implements Serializable {
    private static final long serialVersionUID = 1L;
    protected HashMap<String, String> entries = null;

	public Configuration() {
        this.entries = new HashMap<String, String>();
        //readAllConf("/Users/magl/Google Drive/cmu/14-fall/15640/1155664400/MapReduceFramework/conf");
        readAllConf("conf");
    }

	public Configuration(Configuration conf) {
		this.entries = conf.entries;
	}

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

    public int getInt(String name, int defaultValue) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultValue;
        }

        return Integer.parseInt(valueString);
    }

    public String getSocketAddr(String name, String defaultAddr) {
        String valueString = get(name);
        if (valueString == null) {
            return defaultAddr;
        }

        return valueString;
    }

    public String[] getSocketAddrs(String name) {
        String valueString = get(name);
        if (valueString == null) {
            return null;
        }

        return valueString.split(",");
    }

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
