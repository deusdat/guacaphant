package com.deusdatsolutions.guacaphant.it;

import java.util.Properties;

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

import com.deusdatsolutions.guacaphant.ArangoDBScheme;
import com.deusdatsolutions.guacaphant.ArangoDBTap;

/**
 * Local app to show calling a database named Testing on the ArangoDB server,
 * arangodb.
 * 
 * @author J Patrick Davenport
 *
 */
public class MainInput {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		Properties props = new Properties();
		AppProps.setApplicationJarClass(props, MainInput.class);

		ArangoDBScheme scheme = new ArangoDBScheme("Testing", "FOR u IN ANewCollection FILTER u.GISS_SSTGL != null",
				"RETURN u", "u.GISS_SSTGL DESC", 2, new Fields("GISS_SSTGL", "NCDCland_oceanGL"));

		// ArangoDBTap input = new ArangoDBTap(scheme, "arangodb");
		ArangoDBTap input = new ArangoDBTap(scheme, "10.0.0.185", 8529, "root","", null);

		Scheme outScheme = new TextDelimited(Fields.ALL, ",");
		Hfs devNull = new Hfs(outScheme, "/tmp/righthere", SinkMode.REPLACE);

		Pipe in = new Pipe("FromArango");
		Pipe sort = new GroupBy(in, new Fields("GISS_SSTGL"));
		Pipe out = new Each(sort, new Identity());

		FlowDef flowDef = new FlowDef().addSource(in, input).addTailSink(out,
				devNull);

		Flow flow = new HadoopFlowConnector().connect(flowDef);
		flow.start();
	}
}