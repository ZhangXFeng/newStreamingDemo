package io.transwarp.streaming.business;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

public class HbaseWriter extends Thread {
	private static Log LOG = LogFactory.getLog(HbaseWriter.class);
	private LinkedBlockingQueue<Tuple2<String, String>> rows = new LinkedBlockingQueue<Tuple2<String, String>>();

	private Configuration conf;
	private HTable hTable;
	private int batchSize = 100;
	private static byte[] cf = Bytes.toBytes("f");

	public HbaseWriter() {
		init();
	}

	public void init() {
		conf = HBaseConfiguration.create();
		try {
			String tableName = conf.get("hbase.table.name");
			hTable = new HTable(conf, tableName);
			boolean autoFlush = conf.getBoolean("hbase." + tableName
					+ ".autoFlush", true);
			hTable.setAutoFlush(autoFlush);
			batchSize = conf.getInt("hbase." + tableName + ".batchSize", 100);
		} catch (IOException e) {
			LOG.error("new HTabe() error");
		}

	}

	public void addRow(Tuple2<String, String> row) {
		try {
			rows.put(row);
		} catch (InterruptedException e) {
			LOG.error("addRow error. rowKey=" + row._1 + " values=" + row._2);
		}
	}

	@Override
	public void run() {
		try {
			List<Put> puts = new ArrayList<Put>();
			while (true) {
				Tuple2<String, String> row = rows.take();

				String rowKey = row._1;
				Put put = new Put(Bytes.toBytes(rowKey));
				String kvs = row._2;
				String[] arrKv = kvs.split(",");
				for (int i = 0; i < arrKv.length; i++) {
					String name = arrKv[i].split(":")[0];
					String value = arrKv[i].split(":")[1];
					put.add(cf, name.getBytes(), value.getBytes());
				}
				puts.add(put);

				if (puts.size() == batchSize) {
					hTable.put(puts);
					puts = new ArrayList<Put>();
				}

			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}

}
