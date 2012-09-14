package udf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SampleLoadStorage extends LoadFunc {

	private RecordReader<LongWritable, Text> in = null;

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new TextInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split)
			throws IOException {
		in = reader;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			boolean nextData = in.nextKeyValue();
			if (!nextData) {
				return null;
			}

			LongWritable key = in.getCurrentKey();
			Text text = in.getCurrentValue();

			Tuple nestedTuple1 = TupleFactory.getInstance().newTuple();
			String[] strings1 = text.toString().split("\t");
			for (String string : strings1) {
				nestedTuple1.append(string);
			}
			

			Tuple nestedTuple2 = TupleFactory.getInstance().newTuple();
			String[] strings2 = text.toString().split("\t");
			for (String string : strings2) {
				nestedTuple2.append(string);
			}
			nestedTuple2.append(TupleFactory.getInstance().newTuple("1"));

			Tuple t = TupleFactory.getInstance().newTuple();
			t.append(nestedTuple1);
			t.append(nestedTuple2);

			return t;

		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

}
