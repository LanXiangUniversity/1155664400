package lxu.lxmapreduce.io.format;

import java.io.Serializable;

/**
 * Created by Wei on 11/11/14.
 */
public class Text implements Serializable{
	private String value;

    public Text() {
        this.value = "";
    }

    public Text(String value) {
        this.value = value;
    }

	public String getValue() {
		return this.value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String get() {
		return this.value;
	}

	public void set(String value) {
		this.value = value;
	}

    @Override
    public String toString() {
        return value;
    }
}
