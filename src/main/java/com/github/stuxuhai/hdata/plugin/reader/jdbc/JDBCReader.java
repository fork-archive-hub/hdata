/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.List;

import org.apache.commons.dbutils.DbUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.common.HDataConfigConstants;
import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.DefaultRecord;
import com.github.stuxuhai.hdata.core.Fields;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.core.OutputFieldsDeclarer;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Reader;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.RecordCollector;
import com.github.stuxuhai.hdata.plugin.Splitter;
import com.github.stuxuhai.hdata.util.DataSourceUtils;
import com.github.stuxuhai.hdata.util.JDBCUtils;
import com.google.common.base.Throwables;

public class JDBCReader extends Reader {

    private Connection connection;
    private JDBCIterator sqlPiece;
    private List<String> sqlList;
    private String url;
    private Fields fields = new Fields();
    private int columnCount;
    private int sequence;
    private int[] columnTypes;
    private String nullString = null;
    private String nullNonString = null;
    private String fieldWrapReplaceString = null;
    private DecimalFormat decimalFormat = null;
    private long sqlMetricTime = -1;

    private static final Logger LOGGER = LogManager.getLogger("sql-metric");

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(JobContext context, PluginConfig readerConfig) {
        sqlMetricTime = context.getEngineConfig().getLong(HDataConfigConstants.JDBC_READER_SQL_METRIC_TIME_MS, -1);
        String driver = readerConfig.getString(JDBCReaderProperties.DRIVER);
        url = readerConfig.getString(JDBCReaderProperties.URL);
        String username = readerConfig.getString(JDBCReaderProperties.USERNAME);
        String password = readerConfig.getString(JDBCReaderProperties.PASSWORD);
        nullString = readerConfig.getString(JDBCReaderProperties.NULL_STRING);
        nullNonString = readerConfig.getString(JDBCReaderProperties.NULL_NON_STRING);
        fieldWrapReplaceString = readerConfig.getProperty(JDBCReaderProperties.FIELD_WRAP_REPLACE_STRING);

        String numberFormat = readerConfig.getProperty(JDBCReaderProperties.NUMBER_FORMAT);
        if (numberFormat != null) {
            decimalFormat = new DecimalFormat(numberFormat);
        }

        try {
            connection = DataSourceUtils.getDataSource(driver, url, username, password).getConnection();
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setReadOnly(true);
        } catch (Exception e) {
            throw new HDataException(e);
        }

        sqlPiece = (JDBCIterator) readerConfig.get(JDBCReaderProperties.SQL_ITERATOR);
        sqlList = (List<String>) readerConfig.get(JDBCReaderProperties.SQL);
        if (sqlPiece != null) {
            sequence = (Integer) readerConfig.get(JDBCReaderProperties.SQL_SEQ);
        }

    }

    @Override
    public void execute(RecordCollector recordCollector) {
        try {
            Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (!url.startsWith("jdbc:hive:") && !url.startsWith("jdbc:hive2:")) {
                statement.setFetchSize(Integer.MIN_VALUE);
                statement.setFetchDirection(ResultSet.FETCH_REVERSE);
            }

            if (sqlPiece != null) {
                while (true) {
                    String sql = sqlPiece.getNextSQL(sequence);
                    if (sql == null) {
                        break;
                    }
                    executeSingle(statement, sql, recordCollector);
                }
            } else if (sqlList != null && sqlList.size() > 0) {
                for (String sql : sqlList) {
                    executeSingle(statement, sql, recordCollector);
                }
            } else {
                throw new HDataException("sql 分片 为空");
            }

            statement.close();
        } catch (SQLException e) {
            throw new HDataException(e);
        }
    }

    private void executeSingle(Statement statement, String sql, RecordCollector recordCollector) throws SQLException {
        int rows = 0;
        long startTime = System.currentTimeMillis();
        long endTime = startTime;

        try {
            ResultSet rs = statement.executeQuery(sql);
            endTime = System.currentTimeMillis();

            if (columnCount == 0 || columnTypes == null) {
                ResultSetMetaData metaData = rs.getMetaData();
                columnCount = metaData.getColumnCount();
                columnTypes = new int[columnCount];
                for (int i = 1; i <= columnCount; i++) {
                    fields.add(metaData.getColumnName(i));
                    columnTypes[i - 1] = metaData.getColumnType(i);
                }
            }

            while (rs.next()) {
                Record r = new DefaultRecord(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object o = rs.getObject(i);
                    if (o == null && nullString != null && JDBCUtils.isStringType(columnTypes[i - 1])) {
                        r.add(i - 1, nullString);
                    } else if (o == null && nullNonString != null && !JDBCUtils.isStringType(columnTypes[i - 1])) {
                        r.add(i - 1, nullNonString);
                    } else if (o instanceof String && fieldWrapReplaceString != null) {
                        r.add(i - 1, ((String) o).replace("\r\n", fieldWrapReplaceString).replace("\n", fieldWrapReplaceString));
                    } else {
                        if (decimalFormat != null) {
                            if (o instanceof Double) {
                                r.add(i - 1, Double.valueOf(decimalFormat.format(o)));
                            } else if (o instanceof Float) {
                                r.add(i - 1, Float.valueOf(decimalFormat.format(o)));
                            } else {
                                r.add(i - 1, o);
                            }
                        } else {
                            r.add(i - 1, o);
                        }
                    }
                }
                recordCollector.send(r);
                rows++;
            }
            rs.close();
        } catch (SQLException e) {
            Throwables.propagate(e);
        }

        long spendTime = endTime - startTime;
        if (sqlMetricTime > 0 && spendTime > sqlMetricTime) {
            LOGGER.info("time: {} ms, rows: {}, sql: {}", spendTime, rows, sql);
        }
    }

    @Override
    public void close() {
        DbUtils.closeQuietly(connection);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(fields);
    }

    @Override
    public Class<? extends Splitter> getSplitterClass() {
        return JDBCSplitter.class;
    }
}
