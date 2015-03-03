package com.deusdatsolutions.guacaphant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import cascading.tuple.Tuple;

import com.deusdatsolutions.guacaphant.ArangoDBInputFormat.ArangoDBRecordReader;
import com.deusdatsolutions.guacaphant.ArangoDBInputFormat.ArangoDBSplit;

public class ArangoDBRecordReaderTest {
	private static final String	RETURN_NAME_U_NAME	= "RETURN {name: u.name}";
	JobConf						conf;
	ArangoDBConfiguration		c;
	ArangoDBInputFormat			format;

	@Before
	public void setUp() throws Exception {

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

	@Test
	public void generatesCorrectAQLForSingleSplit() throws IOException {
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

	@Test
	public void generatesCorrectAQLForPartialSplit() throws IOException {
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

	@Test
	public void recordIteration() throws IOException {
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
			Tuple tuple = writer.getTupleEntry().getTuple();
			assertEquals("Patrick", tuple.getString(0));
			assertEquals(1, reader.getPos());
			assertFalse(reader.next(new LongWritable(), writer));
		} finally {
			if (reader != null)
				reader.close();
		}
	}
}
