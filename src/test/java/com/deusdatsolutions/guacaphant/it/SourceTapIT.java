package com.deusdatsolutions.guacaphant.it;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.deusdatsolutions.guacaphant.ArangoDBScheme;
import com.deusdatsolutions.guacaphant.ArangoDBTap;

@RunWith(Suite.class)
@Suite.SuiteClasses({ SourceTapIT.SimpleRead.class,
		SourceTapIT.ComplexRead.class})
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

	public static Set<Map<String, String>> getOutput(final String location,
			String... keys) throws IOException {
		Set<Map<String, String>> result = new HashSet<Map<String, String>>();

		Collection<File> parts = FileUtils.listFiles(new File(location),
				new IOFileFilter() {

					@Override
					public boolean accept(File file) {
						return accept(file, file.getName());
					}

					@Override
					public boolean accept(File dir, String name) {
						return name.startsWith("part");
					}

				}, null);

		for (File part : parts) {
			List<String> readLines = FileUtils.readLines(part);
			for (String row : readLines) {
				String[] split = row.split(",");
				assertEquals("Keys should match split columns", keys.length,
						split.length);
				HashMap<String, String> person = new HashMap<String, String>();
				for (int i = 0; i < split.length; i++) {
					person.put(keys[i], split[i]);
				}
				result.add(person);
			}
		}
		return result;
	}

	public static class SimpleRead {
		private final String				collection	= "SimpleReading";
		private Set<Map<String, String>>	data		= new HashSet<Map<String, String>>();

		@Before
		public void prep() throws Exception {
			driver.createCollection(collection);
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
		public void read() throws Exception {
			Properties props = new Properties();
			AppProps.setApplicationJarClass(props, SimpleRead.class);

			ArangoDBScheme scheme = new ArangoDBScheme(
					driver.getDefaultDatabase(), "FOR u IN " + collection,
					"RETURN {'name': u.name, 'age': u.age}", "u.name DESC", 2,
					new Fields("name", "age"));

			ArangoDBTap input = new ArangoDBTap(scheme, "arangodb", null);

			Scheme outScheme = new TextDelimited(Fields.ALL, ",");
			String outputPath = "/tmp/simpleread";
			Hfs devNull = new Hfs(outScheme, outputPath, SinkMode.REPLACE);

			Pipe in = new Pipe("FromArango");
			Pipe sort = new GroupBy(in, new Fields("name"));
			Pipe out = new Each(sort, new Identity());

			FlowDef flowDef = new FlowDef().addSource(in, input).addTailSink(
					out, devNull);

			Misc.executeFlowDef(flowDef);

			Set<Map<String, String>> output = getOutput(outputPath, "name",
					"age");
			assertEquals(2, output.size());
			assertTrue(output.containsAll(data));
		}

		@After
		public void cleanup() throws ArangoException {
			driver.deleteCollection(collection);
		}
	}

	public static class ComplexRead {
		protected final String					collection	= "ComplexRead";
		protected final Set<Map<String, Object>>	data		= new HashSet<Map<String, Object>>();

		@Before
		public void prep() throws ArangoException {
			driver.createCollection(collection);

			{
				Map<String, String> address1 = new HashMap<String, String>();
				address1.put("city", "Palatka");
				Map<String, Object> person1 = new HashMap<String, Object>();
				person1.put("name", "J Patrick Davenport");
				person1.put("age", "33");
				person1.put("address", address1);
				data.add(person1);
				driver.createDocument(collection, person1);
			}

			{
				Map<String, String> address1 = new HashMap<String, String>();
				address1.put("city", "South Bend");
				Map<String, Object> person1 = new HashMap<String, Object>();
				person1.put("name", "Amber Davenport");
				person1.put("age", "32");
				person1.put("address", address1);
				data.add(person1);
				driver.createDocument(collection, person1);
			}
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Test
		public void read() throws Exception {
			Properties props = new Properties();
			AppProps.setApplicationJarClass(props, SimpleRead.class);

			ArangoDBScheme scheme = new ArangoDBScheme(
					driver.getDefaultDatabase(),
					"FOR u IN " + collection,
					"RETURN {'name': u.name, 'age': u.age, 'address': u.address}",
					"u.name DESC", 2, new Fields("name", "age", "address"));
			ArangoDBTap inputTap = new ArangoDBTap(scheme, "arangodb", null);

			Scheme outScheme = new TextDelimited(Fields.ALL, ",");
			String outputPath = "/tmp/complexread";
			Hfs outputTap = new Hfs(outScheme, outputPath, SinkMode.REPLACE);

			Pipe in = new Pipe("FromArango");
			Pipe cause = new Each(in, new Identity());
			Pipe out = new Each(cause, new Flattener());

			FlowDef flowDef = new FlowDef().addSource(in, inputTap)
					.addTailSink(out, outputTap);

			Misc.executeFlowDef(flowDef);

			Set<Map<String, String>> output = getOutput(outputPath, "name",
					"age", "city");
			assertEquals(2, output.size());
			for (Map<String, Object> doc : data) {
				Map<String, String> address = (Map<String, String>) doc
						.get("address");
				doc.remove("address");
				doc.put("city", address.get("city"));
			}
			assertTrue(output.containsAll(data));
		}

		/**
		 * Flattens the document into easier to work with Tuples.
		 * 
		 * @author J Patrick Davenport
		 *
		 */
		private static final class Flattener extends BaseOperation<Void>
				implements Function<Void> {
			private static final long	serialVersionUID	= 1L;

			public Flattener() {
				super(new Fields("name", "age", "city"));
			}

			@SuppressWarnings("rawtypes")
			@Override
			public void operate(FlowProcess flowProcess,
					FunctionCall<Void> functionCall) {
				TupleEntry arangoDoc = functionCall.getArguments();
				TupleEntry mapped = new TupleEntry(getFieldDeclaration(),
						Tuple.size(3));
				mapped.setString("name", arangoDoc.getString("name"));
				mapped.setString("age", arangoDoc.getString("age"));
				mapped.setString("city", ((TupleEntry) arangoDoc
						.getObject("address")).getString("city"));

				functionCall.getOutputCollector().add(mapped);
			}

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
