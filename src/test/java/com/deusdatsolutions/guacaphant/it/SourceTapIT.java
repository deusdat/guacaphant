package com.deusdatsolutions.guacaphant.it;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.deusdatsolutions.guacaphant.ArangoDBScheme;
import com.deusdatsolutions.guacaphant.ArangoDBTap;

@RunWith(Suite.class)
@Suite.SuiteClasses({ SourceTapIT.SimpleRead.class,
		SourceTapIT.ComplexRead.class })
public class SourceTapIT {
	public static ArangoDriver	driver;

	@BeforeClass
	public static void prep() throws Exception {
		ArangoConfigure aConf = new ArangoConfigure();
		aConf.setHost("arangodb");
		aConf.setDefaultDatabase("GuacaphantIT");
		aConf.init();
		driver = new ArangoDriver(aConf);

		driver.createDatabase(aConf.getDefaultDatabase());
	}

	public static List<Map<String, String>> getOutput(final String location, String...keys)
			throws IOException {
		LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
		List<String> readLines = FileUtils.readLines(new File(location
				+ "/part-00000"));
		for (String row : readLines) {
			String[] split = row.split(",");
			assertEquals("Keys should match split columns", keys.length, split.length);
			HashMap<String, String> person = new HashMap<String, String>();
			for(int i = 0; i < split.length; i++) {
				person.put(keys[i], split[i]);
			}
			result.add(person);
		}
		return result;
	}

	public static class SimpleRead {
		private final String				collection	= "SimpleReading";
		private List<Map<String, String>>	data		= new ArrayList<Map<String, String>>();

		@Before
		public void prep() throws Exception {
			try {
				driver.createCollection(collection);
			} catch (ArangoException ex) {

			}
			Map<String, String> person1 = new HashMap<String, String>();
			person1.put("name", "J Patrick Davenport");
			person1.put("age", "32");
			data.add(person1);

			Map<String, String> person2 = new HashMap<String, String>();
			person2.put("name", "Amber Davenport");
			person2.put("age", "32");
			data.add(person2);

			driver.createDocument(collection, person1);
			driver.createDocument(collection, person2);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Test
		public void read() throws IOException, InterruptedException {
			Properties props = new Properties();
			AppProps.setApplicationJarClass(props, SimpleRead.class);

			ArangoDBScheme scheme = new ArangoDBScheme(
					driver.getDefaultDatabase(), "FOR u IN " + collection,
					"RETURN {'name': u.name, 'age': u.age}", "u.name DESC", 2,
					new Fields("name", "age"));

			// ArangoDBTap input = new ArangoDBTap(scheme, "arangodb");
			ArangoDBTap input = new ArangoDBTap(scheme, "10.0.0.185", 8529,
					"root", "", null);

			Scheme outScheme = new TextDelimited(Fields.ALL, ",");
			String outputPath = "/tmp/simpleread";
			Hfs devNull = new Hfs(outScheme, outputPath, SinkMode.REPLACE);

			Pipe in = new Pipe("FromArango");
			Pipe sort = new GroupBy(in, new Fields("name"));
			Pipe out = new Each(sort, new Identity());

			FlowDef flowDef = new FlowDef().addSource(in, input).addTailSink(
					out, devNull);

			Flow flow = new HadoopFlowConnector().connect(flowDef);
			flow.start();
			Thread.sleep(1000);

			List<Map<String, String>> output = getOutput(outputPath, "name", "age");
			assertEquals(2, output.size());
			assertTrue(output.containsAll(data));
		}

		@After
		public void cleanup() throws ArangoException {
			driver.deleteCollection(collection);
		}
	}

	public static class ComplexRead {
		private final String	collection	= "ComplexRead";

		@Before
		public void prep() throws ArangoException {
			driver.createCollection(collection);
		}

		@Test
		public void read() {

		}

		@After
		public void cleanup() throws ArangoException {
			driver.deleteCollection(collection);
		}
	}

	@AfterClass
	public static void cleanup() throws ArangoException {
		driver.deleteDatabase(driver.getDefaultDatabase());
	}
}
