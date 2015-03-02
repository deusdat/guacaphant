package guac;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Identity;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import com.deusdatsolutions.guacaphant.ArangoDBScheme;
import com.deusdatsolutions.guacaphant.ArangoDBTap;

public class MainOutput {

	public static String getData(final String file) {
		return MainOutput.class.getResource(file).getFile();
	}

	@SuppressWarnings({ "rawtypes" })
	public static void main(String[] args) {
		Properties props = new Properties();
		AppProps.setApplicationJarClass(props, MainInput.class);

		final String inPath = "/Users/jdavenpo/Downloads/TemperatureData.csv";

		Fields s = new Fields("Year", "Month", "Date", "CRU3NH", "CRU3SH",
				"CRU3GL", "CRU3VNH", "CRU3VSH", "CRU3VGL", "HadCRU3NH",
				"HADCRU3SH", "HADCRU3GL", "HadCRU3VNH", "HADCRU3VSH",
				"HADCRU3VGL", "HadSST2NH", "HadSST2SH", "HadSST2GL",
				"GISS_TSNH", "GISS_TSSH", "GISS_TSGL", "GISS_SSTNH",
				"GISS_SSTSH", "GISS_SSTGL", "RSSNH", "RSSSH", "RSSGL", "UAHNH",
				"UAHSH", "UAHGL", "NCDClandNH", "NCDClandSH", "NCDClandGL0",
				"NCDCoceanNH", "NCDCoceanSH", "NCDCoceanGL",
				"NCDCland_oceanNH", "NCDCland_oceanSH", "NCDCland_oceanGL");
		Hfs inputTap = new Hfs(new TextDelimited(s, ",", "\""), inPath);
		 
		ArangoDBTap output = new ArangoDBTap(new ArangoDBScheme("Testing",
		 "ANewCollection"), "10.0.0.185", SinkMode.REPLACE);

		//Hfs output = new Hfs(new TextLine(), "/tmp/hdfscheck", SinkMode.REPLACE);

		Pipe in = new Pipe("FromFile");
		Pipe out = new Each(in, new Identity());

		FlowDef flowDef = new FlowDef().addSource(in, inputTap)
				.addTailSink(out, output);

		Flow flow = new HadoopFlowConnector().connect(flowDef);
		flow.start();
	}
}
