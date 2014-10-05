package remote;

import org.jetbrains.annotations.NotNull;

/**
 * Created by Wei on 10/5/14.
 */
public class TestServiceImpl implements TestService {
	@NotNull
	@Override
	public String test(String str) {
		return str + ", " + str;
	}
}
