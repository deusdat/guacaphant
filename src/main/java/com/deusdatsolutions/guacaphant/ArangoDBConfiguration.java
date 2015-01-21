package com.deusdatsolutions.guacaphant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.arangodb.ArangoConfigure;
import com.arangodb.ArangoDriver;

public class ArangoDBConfiguration {
	public static final String	SERVER			= "mapred.arangodb.server";
	public static final String	PORT			= "mapred.arangodb.port";
	public static final String	DB_NAME			= "mapred.arangodb.db";
	public static final String	USERNAME		= "mapred.arangodb.username";
	public static final String	PASSWORD		= "mapred.arangodb.password";
	public static final String	NUM_SPLITS		= "mapred.arangodb.splits";
	public static final String	MAIN_QUERY		= "mapred.arangodb.main_query";
	public static final String	SORT_CLAUSE		= "mapred.arangodb.sort";
	public static final String	RETURN_CLAUSE	= "mapred.arangodb.return";
	public static final String	PARTITIONS		= "mapred.arangodb.partions";

	public static void set(JobConf conf, String baseUrl) {
		set(conf, baseUrl, null, null);
	}

	public static void set(JobConf conf2, String baseUrl, String username,
			String password) {
		ArangoDBConfiguration c = new ArangoDBConfiguration(conf2);
		c.setUsername(username);
		c.setPassword(password);
	}

	private final Configuration	conf;
	private ArangoDriver		driver;

	public ArangoDBConfiguration(Configuration conf) {
		this.conf = conf;
	}

	public String getServer() {
		return conf.get(SERVER);
	}

	public void setServerUrl(final String url) {
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
		conf.set(USERNAME, username2);
	}

	public String getUsername() {
		return conf.get(USERNAME);
	}

	public String getPassword() {
		return conf.get(PASSWORD);
	}

	public void setPassword(String password2) {
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
		conf.set(RETURN_CLAUSE, returnStatement);
	}
	
	public void setSortStatement(final String sort) {
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

}
