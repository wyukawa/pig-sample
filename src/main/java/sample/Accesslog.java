package sample;

import java.io.IOException;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

public class Accesslog {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			PigServer pigServer = new PigServer(ExecType.LOCAL);
			pigServer.registerScript("pigLatin/accesslog.pig");
			pigServer.shutdown();
		} catch (ExecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
