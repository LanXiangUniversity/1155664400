/**
 * ZIPProcess.java
 * @author Tong Wei (twei1), Guoli Ma (guolim)
 *
 * Description: This is a migratable process. Given an input file, this
 * 	 		    process will generate a zipped output file.
 */

package processes;

import transactionalIO.TransactionalFileInputStream;
import transactionalIO.TransactionalFileOutputStream;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class ZIPProcess implements MigratableProcess {
	private static final long serialVersionUID = -3881329126388008331L;
	private TransactionalFileInputStream inFile;
	private TransactionalFileOutputStream outFile;

	private volatile boolean suspending;

	public ZIPProcess(String[] args) throws Exception {
		if (args.length != 1) {
			System.out.println("usage: ZIPProcess <inputFile>");
			throw new Exception("Invalid Arguments.");
		}

		inFile = new TransactionalFileInputStream(args[0]);
		outFile = new TransactionalFileOutputStream(args[0] + ".gz", false);
	}

	@Override
	public void run() {
		byte[] buffer = new byte[128];
		DataInputStream in = new DataInputStream(inFile);

		try {
			GZIPOutputStream out = new GZIPOutputStream(outFile);

			while (!suspending) {
				int readByte = in.read(buffer);
				if (readByte == -1) { 
					/* EOF */
					System.out.println("ZIP done!");
					break;
				}

				out.write(buffer, 0, readByte);
				/**
				 *  Make zip take longer so that we don't require extremely 
				 *  large files for interesting results.
				 */
				/*try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// ignore it
				}*/
			}

			in.close();
			out.finish();
			out.close();

		} catch (IOException e) {
			System.out.println("ZIPProcess: Error: " + e);
		}
		suspending = false;
	}

	@Override
	public void suspend() {
		suspending = true;
		while (suspending) ;
	}

}
