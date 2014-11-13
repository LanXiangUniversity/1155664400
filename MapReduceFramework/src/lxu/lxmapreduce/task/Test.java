package lxu.lxmapreduce.task;

import lxu.utils.ReflectionUtils;
import sun.reflect.Reflection;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Created by Wei on 11/11/14.
 */
public class Test {
	public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
		Class<?> c = ReflectionUtils.loadClassFromJar("Hi.class", "/Users/parasitew/Documents/testDir/Hi.jar");
		Object hi = c.newInstance();
		Method method = c.getMethod("say");
		method.invoke(hi);
	}


}
