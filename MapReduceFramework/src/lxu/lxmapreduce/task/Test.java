package lxu.lxmapreduce.task;

import lxu.utils.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
