package com.deusdatsolutions.guacaphant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;

public class ArangoDBConfiguration {
	public static final String	SERVER				= "mapred.arangodb.server";
	public static final String	PORT				= "mapred.arangodb.port";
	public static final String	DB_NAME				= "mapred.arangodb.db";
	public static final String	USERNAME			= "mapred.arangodb.username";
	public static final String	PASSWORD			= "mapred.arangodb.password";
	public static final String	NUM_SPLITS			= "mapred.arangodb.splits";
	public static final String	MAIN_QUERY			= "mapred.arangodb.main_query";
	public static final String	SORT_CLAUSE			= "mapred.arangodb.sort";
	public static final String	RETURN_CLAUSE		= "mapred.arangodb.return";
	public static final String	PARTITIONS			= "mapred.arangodb.partions";
	public static final String	TARGET_COLLECTION	= "mapred.arangodb.targetcollection";
	public static final String	WRITE_BATCH_SIZE	= "mapred.arangodb.writebatchsize";

	public static void set(JobConf conf2, String server, int port,
			String username, String password, String targetCollection) {
		ArangoDBConfiguration c = new ArangoDBConfiguration(conf2);
		c.setServerUrl(server);
		c.setPort(port);
		c.setUsername(username);
		c.setPassword(password);
		c.setTargetCollection(targetCollection);
	}

	private final Configuration	conf;
	private ArangoDriver		driver;

	public ArangoDBConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public void setInputFormat() {
		if (conf instanceof JobConf) {
			((JobConf) conf).setInputFormat(ArangoDBInputFormat.class);
		}
	}

	public void setOutputFormat() {
		if (conf instanceof JobConf) {
			((JobConf) conf).setOutputFormat(ArangoDBOutputFormat.class);
		}
	}

	public String getServer() {
		return conf.get(SERVER);
	}

	public void setServerUrl(final String url) {
		if (url != null)
			conf.set(SERVER, url);
	}

	public void setPort(int port) {
		conf.setInt(PORT, port);
	}

	public int getPort() {
		// FIXME Should this really default the port?
		return conf.getInt(PORT, 8529);
	}

	public void setUsername(String username2) {
		if (username2 != null)
			conf.set(USERNAME, username2);
	}

	public String getUsername() {
		return conf.get(USERNAME);
	}

	public String getPassword() {
		return conf.get(PASSWORD);
	}

	public void setPassword(String password2) {
		if (password2 != null)
			conf.set(PASSWORD, password2);
	}

	public int getPartitions() {
		return conf.getInt(PARTITIONS, 1);
	}

	public void setPartitions(final int partitions) {
		conf.setInt(PARTITIONS, partitions);
	}

	public String getMainQuery() {
		return conf.get(MAIN_QUERY);
	}

	public void setMainQuery(final String mainQuery) {
		if (mainQuery != null)
			conf.set(MAIN_QUERY, mainQuery);
	}

	public boolean hasCredentials() {
		return !conf.get(USERNAME, "").isEmpty();
	}

	public String getDatabase() {
		return conf.get(DB_NAME);
	}

	public void setDatabase(final String database) {
		conf.set(DB_NAME, database);
	}

	public String getReturnStatement() {
		return conf.get(RETURN_CLAUSE);
	}

	public void setReturnStatement(final String returnStatement) {
		if (returnStatement != null)
			conf.set(RETURN_CLAUSE, returnStatement);
	}

	public void setSortStatement(final String sort) {
		if (sort != null)
			conf.set(SORT_CLAUSE, sort);
	}

	public String getSortStatement() {
		return conf.get(SORT_CLAUSE);
	}

	public ArangoDriver connection() {
		if (driver == null) {
			ArangoConfigure aConf = new ArangoConfigure();
			aConf.setHost(getServer());
			aConf.setPort(getPort());
			aConf.setDefaultDatabase(getDatabase());
			if (hasCredentials()) {
				aConf.setUser(getUsername());
				aConf.setPassword(getPassword());
			}
			aConf.init();
			driver = new ArangoDriver(aConf);
		}
		return driver;
	}

	public void setConcurrentReads(int splits) {

	}

	public void setTargetCollection(String targetCollection) {
		if (targetCollection != null)
			conf.set(TARGET_COLLECTION, targetCollection);
	}

	public String getTargetCollection() {
		return conf.get(TARGET_COLLECTION);
	}

	public void setWriteBatchSize(final int size) {
		conf.setInt(WRITE_BATCH_SIZE, size);
	}

	/**
	 * 
	 * @return the size of the batch to use when writing. If not set, defaults
	 *         to 1.
	 */
	public int getWriteBatchSize() {
		return conf.getInt(WRITE_BATCH_SIZE, 1);
	}
}
