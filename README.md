# guacaphant


A tap and sink to integrate Hadoop and the ArangoDB via Cascading.


## Example Source Flow
```java
	
package com.deusdatsolutions.guacaphant;
import java.util.Properties;
import org.apache.commons.lang.time.StopWatch;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
public class TestSourceFlow {
public static void main(String[] args) {
StopWatch watch = new StopWatch();
watch.start();
Properties properties = new Properties();
AppProps.setApplicationJarClass(properties, TestSinkFlow.class);
ArangoDBScheme inputScheme = new ArangoDBScheme("192.168.56.10", null,
null, null,"Experimenting",
"FOR p IN DriverTest RETURN p ", new Fields("_id", "upc"), null, false);
ArangoDBTap inTap = new ArangoDBTap(inputScheme);
Scheme outputScheme = new TextDelimited(new Fields("_id", "upc"), ",");
Tap<?, ?, ?> outTap = new FileTap(outputScheme,
"/Users/jdavenpo/products4.csv", SinkMode.REPLACE);
Pipe in = new Pipe("Start");
Pipe ident = new Each(in, new Identity());
FlowDef flowDef = FlowDef.flowDef().addSource(in, inTap)
.addTailSink(ident, outTap)
.setDebugLevel(DebugLevel.VERBOSE);
Flow<?> flow = new LocalFlowConnector().connect(flowDef);
flow.complete();
watch.stop();
System.out.println("Loading took: " + watch.getTime() +"ms");
}
}

```