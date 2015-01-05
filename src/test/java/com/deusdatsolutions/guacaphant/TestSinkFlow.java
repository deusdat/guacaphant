package com.deusdatsolutions.guacaphant;

import java.util.Properties;

import org.apache.commons.lang.time.StopWatch;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.DebugLevel;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class TestSinkFlow {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) {
		StopWatch watch = new StopWatch();
		watch.start();
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, TestSinkFlow.class);

		Scheme inputScheme = new TextDelimited(new Fields("id", "upc"), ",");
		Tap<?, ?, ?> source = new FileTap(inputScheme,
				"/Users/jdavenpo/products1.csv");

		Tap<?, ?, ?> sink = new ArangoDBTap(new ArangoDBScheme("192.168.56.10",
				null, null, null, null,"Experimenting", Fields.ALL, "DriverTest", false));

		Pipe in = new Pipe("Start");

		FlowDef flowDef = FlowDef.flowDef().addSource(in, source)
				.addTailSink(new Each(in, new Identity()), sink)
				.setDebugLevel(DebugLevel.VERBOSE);

		Flow<?> flow = new LocalFlowConnector().connect(flowDef);
		flow.complete();
		
		watch.stop();
		System.out.println("Loading took: " + watch.getTime() +"ms");
	}
}