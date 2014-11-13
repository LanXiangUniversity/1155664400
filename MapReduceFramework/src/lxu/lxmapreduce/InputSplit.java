package lxu.lxmapreduce;

/**
 * Created by Wei on 11/12/14.
 */
public abstract class InputSplit {

	/**
	 * Get the size of the split, so that the input splits can be sorted by size.
	 * @return
	 */
	public abstract long getLength();

	/**
	 * Get the list of nodes by name where the data for the split would be local.
	 * @return
	 */
	public abstract String[] getLocations();
}
