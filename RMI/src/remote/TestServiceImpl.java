package remote;

/**
 * Created by Wei on 10/5/14.
 */
public class TestServiceImpl implements TestService {
	@Override
	public String test(String str) {
		return str + ", " + str;
	}
}
