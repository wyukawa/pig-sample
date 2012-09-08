package sample;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class WordCountTest {

	@Test
	public void testTop2Queries() throws Exception {
		String[] args = { "n=2" };

		PigTest test = new PigTest("pigLatin/top_queries.pig", args);

		String[] input = { "yahoo", "yahoo", "yahoo", "twitter", "facebook",
				"facebook", "linkedin", };

		String[] output = { "(yahoo,3)", "(facebook,2)", };

		test.assertOutput("data", input, "queries_limit", output);
	}

}
