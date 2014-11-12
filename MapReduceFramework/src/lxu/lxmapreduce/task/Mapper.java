package lxu.lxmapreduce.task;

import java.io.IOException;

/**
 * Created by Wei on 11/11/14.
 */
public class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public class Context extends MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
		public Context() {}
	}

	protected void setup(Context context) {

	}

	// Users should override this function.
	protected void map(KEYIN key, VALUEIN value, Context context) {
		context.write((KEYOUT) key, (VALUEOUT) value);
	}

	protected void cleanup(Context context) {

	}

	public void run(Context context) throws IOException {
		setup(context);

		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}

		cleanup(context);
	}

}
