package com.deusdatsolutions.guacaphant;

import java.io.IOException;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;
import com.arangodb.ArangoException;
import com.arangodb.CursorResultSet;

@SuppressWarnings("rawtypes")
public class ArangoDBScheme extends Scheme {
    private final String host;
    private final Integer port;
    private final String username;
    private final String password;
    private final String database;
    private final String collection;
    private final boolean createCollection;
    private final String aql;

    private transient ArangoDriver driver;
    private transient DriverContext ctx;
    private transient CursorResultSet<Map<String, Object>> cursor;

    public ArangoDBScheme(String host, Integer port, String username,
	    String password, String database, String aql, Fields fields,
	    String collection, boolean createCollection) {

	super(fields);
	this.host = host;
	this.port = port;
	this.username = username;
	this.password = password;
	this.database = database;
	this.collection = collection;
	this.createCollection = createCollection;
	this.aql = aql;
	init();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException,
	    ClassNotFoundException {
	init();
    }

    private void init() {
	ArangoConfigure conf = new ArangoConfigure();
	if (this.host != null)
	    conf.setHost(host);
	if (this.database != null)
	    conf.setDefaultDatabase(database);
	if (port != null)
	    conf.setPort(port);
	if (username != null)
	    conf.setUser(username);
	if (password != null)
	    conf.setPassword(password);

	conf.init();
	driver = new ArangoDriver(conf);

	ctx = new DriverContext(driver, collection, createCollection);
    }

    protected String getPath() {
	return host + port + database + collection + aql;
    }

    @Override
    public void sourceConfInit(FlowProcess flowProcess, Tap tap, Object conf) {
	System.out.println("Hello");
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sinkCleanup(FlowProcess flowProcess, SinkCall sinkCall)
	    throws IOException {
	super.sinkCleanup(flowProcess, sinkCall);
    }

    @Override
    public void sinkConfInit(FlowProcess flowProcess, Tap tap, Object conf) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public Fields retrieveSourceFields(FlowProcess flowProcess, Tap tap) {
	setSourceFields(getSourceFields());
	return super.retrieveSourceFields(flowProcess, tap);
    }

    @Override
    public boolean source(FlowProcess flowProcess, SourceCall sourceCall)
	    throws IOException {
	if(!cursor.hasNext())
	    return false;
	Map<String, Object> input = cursor.next();
	TupleEntry te = sourceCall.getIncomingEntry();
	
	TupleEntry doc = InteropTools.createTupleEntry(input);
	te.getTuple().clear();
	te.setTuple(doc.selectTuple(te.getFields()));
	return true;
    }

    @Override
    public void sink(FlowProcess flowProcess, SinkCall sinkCall)
	    throws IOException {
	TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
	ArangoDBTupleEntryCollector outputCollector = (ArangoDBTupleEntryCollector) sinkCall
		.getOutput();

	outputCollector.collect(ctx, tupleEntry);
    }

    protected ArangoDriver getDriver() {
	return driver;
    }

    @SuppressWarnings("unchecked")
    public CursorResultSet<Map<String, Object>> executeQuery()
	    throws IOException {
	if (cursor == null) {
	    try {
		cursor = driver.executeQueryWithResultSet(aql, (Map) null,
			Map.class, false, 10);
	    } catch (ArangoException e) {
		throw new IOException(e);
	    }
	}
	return cursor;
    }
}
