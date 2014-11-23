package lxu.lxmapreduce.io;

/**
 * Created by Wei on 11/11/14.
 */

import lxu.lxdfs.metadata.LocatedBlock;

import java.io.IOException;
import java.util.List;

/**
 * Break the records into key/value pairs for input to the Mapper.
 *
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public abstract class RecordReader<KEYIN, VALUEIN> {

    public abstract void initialize(List<String> inputFiles, List<LocatedBlock> locatedBlockses) throws IOException;

    /**
     * Get the current value.
     */
    public abstract VALUEIN getCurrentValue();

    /**
     * Get the current key.
     */
    public abstract KEYIN getCurrentKey();

    /**
     * Read the next key value pair.
     *
     * @return
     */
    public abstract boolean nextKeyValue() throws IOException;

    /**
     * Close the record reader.
     */
    public abstract void close() throws IOException;
}
