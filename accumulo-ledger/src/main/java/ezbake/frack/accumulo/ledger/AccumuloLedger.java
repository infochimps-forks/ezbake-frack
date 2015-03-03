/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.frack.accumulo.ledger;

import java.util.Map;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.security.thrift.AppNotRegisteredException;
import ezbakehelpers.accumulo.AccumuloHelper;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.frack.api.FrackLedger;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.security.client.EzbakeSecurityClient;

import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.client.MutationsRejectedException;

/**
 * Accumulo implementation of FrackLedger
 */
public class AccumuloLedger implements FrackLedger {

	private String pipelineId;
	private Connector connector;
	private LedgerCrypto crypto;
	private Properties props;
	private String applicationSecurityId;
	private EzbakeSecurityClient securityClient;

	private static long maxMemory, maxLatency;
	private static int queryThreads, writeThreads;
	private static final int DEFAULT_QUERY_THREADS = 1;
	private static final int DEFAULT_WRITE_THREADS = 1;
	private static final long DEFAULT_MAX_LATENCY = 1000l;
	private static final long DEFAULT_MAX_MEMORY = 2560000l;
	
	private static String TABLE_NAME = "frack_ledger";
	private static final String DEFUALT_COLUMN_SEPARATOR = "---";
	private static final String MAX_MEMORY_KEY = "accumulo.ledger.max.memory";
	private static final String MAX_LATENCY_KEY = "accumulo.ledger.max.latency";
	private static final String QUERY_THREADS_KEY = "accumulo.ledger.max.query.threads";
	private static final String WRITE_THREADS_KEY = "accumulo.ledger.max.write.threads";

	private static final long serialVersionUID = 1L;
	private static final Logger log = LoggerFactory.getLogger(AccumuloLedger.class);

	public AccumuloLedger(String pipelineId, Properties props) {
		this.props = props;
		this.pipelineId = pipelineId;
		this.initialize();
	}

	public void initialize() { 
		try {
			log.debug("Initializing Frack accumulo-ledger");

			this.connector = new AccumuloHelper(props).getConnector();

			applicationSecurityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
			log.debug("Initializing authorizations for [" + applicationSecurityId + "].");
			this.securityClient = new EzbakeSecurityClient(this.props);
			this.crypto = new LedgerCrypto(props);

            EzProperties ezProps = new EzProperties(props, true);
			AccumuloLedger.maxLatency = ezProps.getLong(MAX_LATENCY_KEY, DEFAULT_MAX_LATENCY);
			AccumuloLedger.maxMemory = ezProps.getLong(MAX_MEMORY_KEY, DEFAULT_MAX_MEMORY);
			AccumuloLedger.queryThreads = ezProps.getInteger(QUERY_THREADS_KEY, DEFAULT_QUERY_THREADS);
			AccumuloLedger.writeThreads = ezProps.getInteger(WRITE_THREADS_KEY, DEFAULT_WRITE_THREADS);
			log.debug("Initialized with max memory of {}, max latency of {}, {} query threads, and {} write threads.", maxMemory, maxLatency, queryThreads, writeThreads);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void insert(String messageId, String fromPipe, String toPipe, Visibility visibility, byte[] message) throws IOException {
		BatchWriter writer = null;
		String pipeName = fromPipe + DEFUALT_COLUMN_SEPARATOR + toPipe ;
		Mutation mutator = new Mutation(new Text(messageId));
		byte[] encryptedMessage = this.crypto.encrypt(message);
		mutator.put(new Text(this.pipelineId), new Text(pipeName), new ColumnVisibility(visibility.getFormalVisibility()), System.currentTimeMillis(), new Value(encryptedMessage));

		try { 
			writer = this.getBatchWriter();
			writer.addMutation(mutator);
			writer.flush();
			log.debug("Inserted {} for {}.", messageId, pipeName);
		} catch (TableNotFoundException | MutationsRejectedException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (MutationsRejectedException e) {
				log.error("Error on closing BatchWriter ", e);
			}
		}

	}

	@Override
	public byte[] get(String messageId, String fromPipe, String toPipe, Visibility visibility) throws IOException {
		Scanner scanner = null;
		byte[] value = null;
		String pipeName = fromPipe + DEFUALT_COLUMN_SEPARATOR + toPipe ;

		try { 
			scanner = this.getScanner();
			scanner.setRange(Range.exact(new Text(messageId), new Text(this.pipelineId), new Text(pipeName), new Text(visibility.getFormalVisibility())));
			log.debug("Attempting fetch on range - " + scanner.getRange());

			for (Map.Entry<Key,Value> row : scanner) { 
				log.debug("Key: " + row.getKey().toString());
				value = this.crypto.decrypt(row.getValue().get());
			}
		} catch (TableNotFoundException e) { 
			throw new RuntimeException(e);
		}
		return value; 
	}

	@Override
	public boolean hasQueuedItems(String pipelineId, long endTime) { 
		Scanner scanner = null;
		boolean hasItems = false;
		try { 
			scanner = this.getScanner();
			scanner.fetchColumnFamily(new Text(this.pipelineId));

			IteratorSetting is = new IteratorSetting(15, "tsfilter", TimestampFilter.class);
			TimestampFilter.setEnd(is, endTime, true);
			scanner.addScanIterator(is);

			hasItems = scanner.iterator().hasNext();
			log.debug("{} has rows: {} ", pipelineId, hasItems);
		} catch (TableNotFoundException e) { 
			throw new RuntimeException(e);
		}	
		return hasItems;
	}

	@Override
	public Map<String, Visibility> getQueuedItems(String fromPipe, String toPipe, long endTime) throws IOException {
		Scanner scanner = null;
		String pipeName = fromPipe + DEFUALT_COLUMN_SEPARATOR + toPipe;
		ConcurrentHashMap<String, Visibility> values = new ConcurrentHashMap<>();

		try { 
			scanner = this.getScanner();
			scanner.fetchColumn(new Text(this.pipelineId), new Text(pipeName));

			IteratorSetting is = new IteratorSetting(15, "tsfilter", TimestampFilter.class);
			TimestampFilter.setEnd(is, endTime, true);
			scanner.addScanIterator(is);

			for (Map.Entry<Key,Value> row : scanner) { 
				values.put(row.getKey().getRow().toString(), new Visibility().setFormalVisibility(row.getKey().getColumnVisibility().toString()));
			}
			log.debug("{} has {} number of rows: ", pipeName, values.size());
		} catch (TableNotFoundException e) { 
			throw new RuntimeException(e);
		}
		return values; 
	}

	@Override
	public void delete(String messageId, String fromPipe, String toPipe,  Visibility visibility) throws IOException {
		BatchDeleter deleter = null; 
		String pipeName = fromPipe + DEFUALT_COLUMN_SEPARATOR + toPipe;

		try { 
			deleter = this.getBatchDeleter();
			List<Range> ranges = new ArrayList<Range>();
			ranges.add(Range.exact(new Text(messageId), new Text(this.pipelineId), new Text(pipeName), new Text(visibility.getFormalVisibility())));
			deleter.setRanges(ranges);
			deleter.delete();
			log.debug("Deleted {} for {}.", messageId, pipeName);
		} catch (TableNotFoundException e) { 
			throw new RuntimeException(e);
		} catch (MutationsRejectedException e) {
			log.warn("Error on deleting messageId " + messageId, e);
		} finally {
			if (deleter != null) {
				deleter.close();
			}
		}	
	}

	/**
	 * Ensures that the table is in accumulo and attempts to create it if it does not exist.
	 *
	 * @throws TException
	 */
	private void ensureTable() throws TException {
		try {
			if (!connector.tableOperations().exists(TABLE_NAME)) {
				log.warn(TABLE_NAME + " does not exist.  Attempting to create...");
				connector.tableOperations().create(TABLE_NAME, true);
				log.warn(TABLE_NAME + " was created.");
			}
		} catch (TableExistsException e) {
			log.debug(TABLE_NAME + " already exists.");
		} catch (Exception e) {
			log.error(TABLE_NAME + " either does not exist and couldn't be created, or something else is broken", e);
			throw new RuntimeException(e);
		}
	}

	private BatchWriter getBatchWriter() throws TableNotFoundException { 
		BatchWriter writer = this.connector.createBatchWriter(TABLE_NAME, maxMemory, maxLatency, writeThreads);
		return writer;
	}

	private Scanner getScanner() throws TableNotFoundException { 
		Authorizations authorizations = this.getAuthorizations();
		Scanner scanner = this.connector.createScanner(TABLE_NAME, authorizations); 
		return scanner;
	}

	@SuppressWarnings("unused")
	private BatchScanner getBatchScanner() throws TableNotFoundException { 
		Authorizations authorizations = this.getAuthorizations();
		BatchScanner scanner = this.connector.createBatchScanner(TABLE_NAME, authorizations, queryThreads);
		return scanner;
	}

	private BatchDeleter getBatchDeleter() throws TableNotFoundException { 
		Authorizations authorizations = this.getAuthorizations();
		BatchDeleter deleter = this.connector.createBatchDeleter(TABLE_NAME, authorizations, queryThreads, maxMemory, maxLatency, writeThreads);
		return deleter;
	}

	private Authorizations getAuthorizations() { 
		Authorizations authorizations;
		List<byte[]> auths = new ArrayList<byte[]>();
		
		try { 
			EzSecurityToken token = this.securityClient.fetchAppToken();
			
			for (String auth : token.getAuthorizations().getFormalAuthorizations()) {
				auths.add(auth.getBytes());
			}
			authorizations = new Authorizations(auths);
		} catch (EzSecurityTokenException e) {
			log.error("Could not initialize EzSecurityToken ", e);
			throw new RuntimeException(e);
		}
		return authorizations;
	}
}
