package com.deusdatsolutions.guacaphant;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.tap.TapException;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntrySchemeCollector;

public class ArangoDBTapCollector extends
		TupleEntrySchemeCollector<JobConf, Object> implements
		OutputCollector<TupleEntry, Void> {
	private static final Logger				LOG	= LoggerFactory
														.getLogger(ArangoDBTapCollector.class);

	private ArangoDBTap						tap;
	private JobConf							conf;
	private FlowProcess<JobConf>			hadoopFlowProcess;
	private RecordWriter<TupleEntry, Void>	writer;

	public ArangoDBTapCollector(FlowProcess<JobConf> flowProcess,
			ArangoDBTap tap) {
		super(flowProcess, tap.getScheme());
		this.tap = tap;
		this.conf = new JobConf(flowProcess.getConfigCopy());
		this.hadoopFlowProcess = flowProcess;
	}

	@Override
	public void collect(TupleEntry te, Void arg1) throws IOException {
		if (hadoopFlowProcess instanceof HadoopFlowProcess)
			((HadoopFlowProcess) hadoopFlowProcess).getReporter().progress();

		writer.write(te, arg1);
	}

	@Override
	public void prepare() {
		try {
			initialize();
		} catch (IOException e) {
			throw new CascadingException(e);
		}
		super.prepare();
	}

	@Override
	public void close() {
		LOG.info("closing tap collector for: {}", tap);
		try {
			writer.close(Reporter.NULL);
		} catch (IOException e) {
			throw new TapException(e);
		} finally {
			super.close();
		}
	}

	private void initialize() throws IOException {
		tap.sinkConfInit(hadoopFlowProcess, conf);
		OutputFormat outputFormat = conf.getOutputFormat();
		writer = outputFormat.getRecordWriter(null, conf, tap.getIdentifier(),
				Reporter.NULL);
		sinkCall.setOutput(this);
	}
}
