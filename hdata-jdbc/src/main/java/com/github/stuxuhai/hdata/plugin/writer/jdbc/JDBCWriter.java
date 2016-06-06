package com.github.stuxuhai.hdata.plugin.writer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.util.JdbcUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class JDBCWriter extends Writer {

	private Connection connection = null;
	private PreparedStatement statement = null;
	private int count;
	private int batchInsertSize;
	private Fields columns;
	private String table;
	private Map<String, Integer> columnTypes;
	private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(Constants.DATE_FORMAT_STRING);
	private static final int DEFAULT_BATCH_INSERT_SIZE = 10000;
	private static final Logger LOG = LogManager.getLogger(JDBCWriter.class);

	@Override
	public void prepare(JobContext context, PluginConfig writerConfig) {
		columns = context.getFields();
		String driver = writerConfig.getString(JBDCWriterProperties.DRIVER);
		Preconditions.checkNotNull(driver, "JDBC writer required property: driver");

		String url = writerConfig.getString(JBDCWriterProperties.URL);
		Preconditions.checkNotNull(url, "HDFS writer required property: url");

		String username = writerConfig.getString(JBDCWriterProperties.USERNAME);
		String password = writerConfig.getString(JBDCWriterProperties.PASSWORD);
		String table = writerConfig.getString(JBDCWriterProperties.TABLE);
		Preconditions.checkNotNull(table, "HDFS writer required property: table");

		this.table = table;
		batchInsertSize = writerConfig.getInt(JBDCWriterProperties.BATCH_INSERT_SIZE, DEFAULT_BATCH_INSERT_SIZE);
		if (batchInsertSize < 1) {
			batchInsertSize = DEFAULT_BATCH_INSERT_SIZE;
		}

		try {
			connection = JdbcUtils.getConnection(driver, url, username, password);
			connection.setAutoCommit(false);
			columnTypes = JdbcUtils.getColumnTypes(connection, table);

			String sql = null;
			if (columns != null) {
				String[] placeholder = new String[columns.size()];
				Arrays.fill(placeholder, "?");
				sql = String.format("INSERT INTO %s(%s) VALUES(%s)", table, "`" + Joiner.on("`, `").join(columns) + "`",
						Joiner.on(", ").join(placeholder));
				LOG.debug(sql);
				statement = connection.prepareStatement(sql);
			}
		} catch (Exception e) {
			throw new HDataException(e);
		}
	}

	@Override
	public void execute(Record record) {
		try {
			if (statement == null) {
				String[] placeholder = new String[record.size()];
				Arrays.fill(placeholder, "?");
				String sql = String.format("INSERT INTO %s VALUES(%s)", table, Joiner.on(", ").join(placeholder));
				LOG.debug(sql);
				statement = connection.prepareStatement(sql);
			}

			for (int i = 0, len = record.size(); i < len; i++) {
				if (record.get(i) instanceof Timestamp
						&& !Integer.valueOf(Types.TIMESTAMP).equals(columnTypes.get(columns.get(i).toLowerCase()))) {
					statement.setObject(i + 1, DATE_FORMAT.format(record.get(i)));
				} else {
					statement.setObject(i + 1, record.get(i));
				}
			}

			count++;
			statement.addBatch();

			if (count % batchInsertSize == 0) {
				count = 0;
				statement.executeBatch();
				connection.commit();
			}
		} catch (SQLException e) {
			throw new HDataException(e);
		}
	}

	@Override
	public void close() {
		try {
			if (connection != null && statement != null && count > 0) {
				statement.executeBatch();
				connection.commit();
			}

			if (statement != null) {
				statement.close();
			}
		} catch (SQLException e) {
			throw new HDataException(e);
		} finally {
			DbUtils.closeQuietly(connection);
		}
	}
}
