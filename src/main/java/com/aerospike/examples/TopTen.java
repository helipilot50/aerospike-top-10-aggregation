package com.aerospike.examples;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;


/**
@author Peter Milne
*/
public class TopTen {
	private static final String EVENT_ID_BIN = "eventid";
	private static final String TIME_BIN = "time";
	private static final String LIST_BIN = "interests";
	private static final int MAX_RECORDS = 100000;
	private AerospikeClient client;
	private String seedHost;
	private int port;
	private String namespace;
	private String set;
	private WritePolicy writePolicy;
	private Policy policy;
	
	private static final String EVENT_ID_PREFIX = "Event:";

	private static Logger log = Logger.getLogger(TopTen.class);
	private int total;
	public TopTen(String host, int port, String namespace, String set)  {
		this.client = new AerospikeClient(host, port);
		this.seedHost = host;
		this.port = port;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public TopTen(AerospikeClient client, String namespace, String set)  {
		this.client = client;
		this.namespace = namespace;
		this.set = set;
		this.writePolicy = new WritePolicy();
		this.policy = new Policy();
	}
	public static void main(String[] args) {
		try {
			Options options = new Options();
			options.addOption("h", "host", true, "Server hostname (default: 127.0.0.1)");
			options.addOption("p", "port", true, "Server port (default: 3000)");
			options.addOption("n", "namespace", true, "Namespace (default: test)");
			options.addOption("s", "set", true, "Set (default: demo)");
			options.addOption("u", "usage", false, "Print usage.");
			options.addOption("l", "load", false, "Load data.");
			options.addOption("q", "query", false, "Aggregate with query.");
			options.addOption("a", "all", false, "Aggregate all using ScanAggregate.");
			options.addOption("S", "scan", false, "Scan all for testing.");

			CommandLineParser parser = new PosixParser();
			CommandLine cl = parser.parse(options, args, false);


			String host = cl.getOptionValue("h", "127.0.0.1");
			String portString = cl.getOptionValue("p", "3000");
			int port = Integer.parseInt(portString);
			String namespace = cl.getOptionValue("n", "test");
			String set = cl.getOptionValue("s", "demo");
			log.debug("Host: " + host);
			log.debug("Port: " + port);
			log.debug("Namespace: " + namespace);
			log.debug("Set: " + set);


			TopTen as = new TopTen(host, port, namespace, set);
			/*
			 * Create index for query
			 * Index creation only needs to be done once and can be done using AQL or ASCLI also
			 */
			IndexTask it = as.client.createIndex(null, as.namespace, as.set, "top-10", TIME_BIN, IndexType.NUMERIC);
			it.waitTillComplete();
			/*
			 * Register UDF module
			 * Registration only needs to be done after a change in the UDF module.
			 */
			RegisterTask rt = as.client.register(null, "udf/leaderboard.lua", "leaderboard.lua", Language.LUA);
			rt.waitTillComplete();
			
			if (cl.hasOption("l")) {
				as.populateData();
				return;
			} else if (cl.hasOption("q")) {
				as.queryAggregate();
				return;
			} else if (cl.hasOption("a")) {
				as.scanAggregate();
				return;
			} else if (cl.hasOption("S")) {
				as.scanAll();
				return;
			} else {
				logUsage(options);
			}
			

		} catch (Exception e) {
			log.error("Critical error", e);
		}
	}
	private void scanAll(){
		long start = System.currentTimeMillis();
		total = 0;
		this.client.scanAll(null, this.namespace, this.set, new ScanCallback() {
			
			@Override
			public void scanCallback(Key key, Record record) throws AerospikeException {
				total++;
				
			}
		}, EVENT_ID_BIN, TIME_BIN);
		long stop = System.currentTimeMillis();
		System.out.println(String.format("Scan time for %d is %d milliseconds", total, (stop - start)));
	}
	private void scanAggregate() {
		long now = System.currentTimeMillis();
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(EVENT_ID_BIN, TIME_BIN);
		aggregate(stmt);
		long completed = System.currentTimeMillis();
		System.out.println(String.format("ScanAggregate completed in: %dms", completed-now));
	}
	public void queryAggregate() {
		long now = System.currentTimeMillis();
		long yesterday = now - 24 * 60 * 60 * 1000;
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.set);
		stmt.setBinNames(EVENT_ID_BIN, TIME_BIN);
		stmt.setFilters(Filter.range(TIME_BIN, yesterday, now));
		aggregate(stmt);
		long completed = System.currentTimeMillis();
		System.out.println(String.format("QueryAggregate completed in: %dms", completed-now));
	}
	
	private void aggregate(Statement stmt){
		
		ResultSet rs = this.client.queryAggregate(null, stmt, "leaderboard", "top", Value.get(10));
		
		while (rs.next()){
			List<Map<String, Object>> result =  (List<Map<String, Object>>) rs.getObject();
			for (Map<String, Object> element : result){
				System.out.println(element);
			}
		}
	}
	
	public void populateData(){
		for (int index = 1; index <= MAX_RECORDS; index++){
			String keyString = EVENT_ID_PREFIX + index;
			long now = System.currentTimeMillis();
			Key key = new Key(this.namespace, this.set, keyString);
			Bin eventIdBin = new Bin(EVENT_ID_BIN, keyString);
			Bin eventTSBin = new Bin(TIME_BIN, now);
			Bin eventListBin = Bin.asList(LIST_BIN, getList());
			this.client.put(null, key, eventIdBin, eventTSBin, eventListBin);
			System.out.println("Created: " + keyString);
		}
	}
	
	String[] interests = {"cats", "dogs", "mice", "birds", "lizards"};
	Random rand = new Random();
	
	private List<String> getList(){
		ArrayList<String> list = new ArrayList<String>();
		int limit = rand.nextInt(5);
		for (int i = 0; i < limit; i++){
			list.add(interests[rand.nextInt(limit)]);
		}
		return list;
	}
	/**
	 * Write usage to console.
	 */
	private static void logUsage(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		String syntax = TopTen.class.getName() + " [<options>]";
		formatter.printHelp(pw, 100, syntax, "options:", options, 0, 2, null);
		log.info(sw.toString());
	}

	

}