package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * The scheme used to interact with ArangoDB's query API.
 * 
 * The mainQuery is the bulk of the query with the actual logic in it.
 * <p>
 * For example: mainQuery = "FOR u IN users FILTER u.name == 'Bob'"
 * </p>
 * 
 * The returnClause is the return statement
 * <p>
 * For example: RETURN u
 * </p>
 * 
 * The sortClause is the snippet of a sort statement for the results. This is
 * necessary when the split size is designated.
 * <p>
 * For example: u.last_name
 * </p>
 * 
 * 
 * @author jdavenpo
 *
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ArangoDBScheme extends
		Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
	private static final long	serialVersionUID	= 1L;
	private String				database;
	private String				mainQuery;
	private String				returnClause;
	private String				sortClause;
	private int					concurrentReads;

	/**
	 * Constructs an instance of the scheme with the fields set to
	 * {@link Fields.UNKNOW}.
	 * 
	 * @param database
	 * @param mainQuery
	 * @param sortClause
	 * @param concurrentReads
	 */
	public ArangoDBScheme(String database, String mainQuery,
			String returnClause, String sortClause, int concurrentReads) {
		this(database, mainQuery, returnClause, sortClause, concurrentReads,
				Fields.UNKNOWN);
	}

	/**
	 * Constructs a scheme with a fixed schema for the top level document.
	 * 
	 * @param database
	 * @param mainQuery
	 * @param sortClause
	 * @param concurrentReads
	 * @param fields
	 */
	public ArangoDBScheme(String database, String mainQuery,
			String returnClause, String sortClause, int concurrentReads,
			Fields fields) {
		super(fields);
		this.database = database;
		this.mainQuery = mainQuery;
		this.returnClause = returnClause;
		this.sortClause = sortClause;
		this.concurrentReads = concurrentReads;
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
		conf.setInputFormat(ArangoDBInputFormat.class);
		ArangoDBConfiguration c = new ArangoDBConfiguration(conf);
		c.setDatabase(database);
		c.setMainQuery(mainQuery);
		c.setSortStatement(sortClause);
		c.setPartitions(concurrentReads);
		c.setReturnStatement(returnClause);
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> flowProcess,
			Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
	}

	@Override
	public void sourcePrepare(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		RecordReader input = sourceCall.getInput();
		Object[] pair = new Object[] { input.createKey(), input.createValue() };
		sourceCall.setContext(pair);
	}

	@Override
	public boolean source(FlowProcess<JobConf> flowProcess,
			SourceCall<Object[], RecordReader> sourceCall) throws IOException {
		Object key = sourceCall.getContext()[0];
		Object value = sourceCall.getContext()[1];
		boolean result = sourceCall.getInput().next(key, value);

		if (!result) {
			return false;
		}

		TupleEntry t = ((ArangoDBWriter) value).getTupleEntry();
		copyTE(t, sourceCall.getIncomingEntry());
		return true;
	}

	public void copyTE(final TupleEntry source, final TupleEntry target) {
		final Fields sourceFields = this.getSourceFields();
		if (sourceFields.equals(Fields.UNKNOWN)) {
			target.setTuple(source.getTuple());
		} else {
			final Iterator<Comparable> fieldsIt;
			fieldsIt = sourceFields.iterator();
			while (fieldsIt.hasNext()) {
				Comparable field = fieldsIt.next();
				target.setRaw(field, source.getObject(field));
			}
		}

	}

	@Override
	public void sink(FlowProcess<JobConf> flowProcess,
			SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
	}

	public String getDatabase() {
		return database;
	}

}
