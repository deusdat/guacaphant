package com.deusdatsolutions.guacaphant.it;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;

public class Misc {
	private Misc() {}
	
	public static void executeFlowDef(FlowDef def) throws Exception {
		@SuppressWarnings("rawtypes")
		Flow flow = new HadoopFlowConnector().connect(def);
		flow.complete();
	}
}
