package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;

import junit.framework.TestCase;
import cascading.tuple.Tuple;

import com.deusdatsolutions.guacaphant.ArangoDBInputFormat.ArangoDBRecordReader;
import com.deusdatsolutions.guacaphant.ArangoDBInputFormat.ArangoDBSplit;

public class ArangoDBRecordReaderTest extends TestCase {
	private static final String	RETURN_NAME_U_NAME	= "RETURN {name: u.name}";
	JobConf						conf;
	ArangoDBConfiguration		c;
	ArangoDBInputFormat			format;

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		conf = new JobConf();
		c = new ArangoDBConfiguration(conf);
		c.setDatabase("Testing");
		c.setServerUrl("arangodb");
		c.setPort(8529);
		c.setMainQuery("FOR u in Users");
		c.setReturnStatement(RETURN_NAME_U_NAME);
		format = new ArangoDBInputFormat();
		format.configure(conf);
	}

	public void testGenerateCorrectAQLForSingleSplit() throws IOException {
		ArangoDBSplit split = new ArangoDBSplit(0, 0, false);
		ArangoDBRecordReader reader = null;
		try {
			reader = format.new ArangoDBRecordReader(split, conf);
			assertEquals("FOR u in Users\n" + RETURN_NAME_U_NAME,
					reader.getAQL());
		} finally {
			if (reader != null)
				reader.close();
		}

	}

	public void testGenerateCorrectAQLForPartialSplit() throws IOException {
		ArangoDBSplit split = new ArangoDBSplit(0, 100, true);
		String sort = "u.name DESC";
		c.setSortStatement(sort);
		format.configure(conf);
		ArangoDBRecordReader reader = null;
		try {
			reader = format.new ArangoDBRecordReader(split, conf);
			assertEquals("FOR u in Users\nSORT " + sort + "\nLIMIT 0,100\n"
					+ RETURN_NAME_U_NAME, reader.getAQL());
		} finally {
			if (reader != null)
				reader.close();
		}

	}

	public void testRecordIteration() throws IOException {
		ArangoDBSplit split = new ArangoDBSplit(0, 1, true);
		ArangoDBRecordReader reader = null;
		String sort = "u.name DESC";
		c.setSortStatement(sort);
		format.configure(conf);
		
		try {
			reader = format.new ArangoDBRecordReader(split, conf);
			assertEquals(0, reader.getPos());
			ArangoDBWriter writer = new ArangoDBWriter();
			reader.next(new LongWritable(), writer);
			Tuple tuple = writer.getTuple();
			assertEquals("Patrick", tuple.getString(0));
			assertEquals(1, reader.getPos());
			assertFalse(reader.next(new LongWritable(), writer));
		} finally {
			if (reader != null)
				reader.close();
		}
	}
}
