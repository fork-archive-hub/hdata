package com.github.stuxuhai.hdata.plugin.writer.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.Fields;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.jdbc.JdbcUtils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class JDBCWriter extends Writer {

    private static final int DEFAULT_BATCH_INSERT_SIZE = 10000;
    private static final Logger LOG = LogManager.getLogger(JDBCWriter.class);

    private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(Constants.DATE_FORMAT_STRING);

    private Connection connection;
    private PreparedStatement statement;
    private int count;
    private int batchInsertSize;
    private Fields columns;
    private String[] schema;
    private List<String> upsertColumns;
    private String table;
    private String keywordEscaper;
    private Map<String, Integer> columnTypes;

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        this.keywordEscaper = writerConfig.getProperty(JDBCWriterProperties.KEYWORD_ESCAPER, "`");
        this.columns = context.getFields();

        this.table = writerConfig.getString(JDBCWriterProperties.TABLE);
        Preconditions.checkNotNull(table, "JDBC writer required property: table");

        String schemaStr = writerConfig.getString("schema");
        if (StringUtils.isNotBlank(schemaStr)) {
            this.schema = schemaStr.split(",");
        }

        String upsertColumnsStr = writerConfig.getString(JDBCWriterProperties.UPSERT_COLUMNS);
        if (StringUtils.isNotBlank(upsertColumnsStr)) {
            this.upsertColumns = Arrays.asList(upsertColumnsStr.trim().split(","));
        }

        this.batchInsertSize = writerConfig.getInt(JDBCWriterProperties.BATCH_INSERT_SIZE, DEFAULT_BATCH_INSERT_SIZE);
        if (batchInsertSize < 1) {
            batchInsertSize = DEFAULT_BATCH_INSERT_SIZE;
        }

        prepareConnection(writerConfig);

        try {
            columnTypes = JdbcUtils.getColumnTypes(connection, table, keywordEscaper);
        } catch (Exception e) {
            throw new HDataException(e);
        }

        List<String> insertColumns;
        if (this.schema != null) {
            insertColumns = Arrays.asList(this.schema);
        } else if (this.columns != null) {
            insertColumns = this.columns;
        } else {
            // TODO: read table columns by JDBC
            insertColumns = null;
        }
        if (insertColumns != null) {
            prepareStatement(buildInsertSql(table, insertColumns, this.upsertColumns));
        }
    }

    private void prepareConnection(PluginConfig writerConfig) {
        String url = writerConfig.getString(JDBCWriterProperties.URL);
        Preconditions.checkNotNull(url, "JDBC writer required property: url");
        String driver = writerConfig.getString(JDBCWriterProperties.DRIVER);
        Preconditions.checkNotNull(driver, "JDBC writer required property: driver");

        String username = writerConfig.getString(JDBCWriterProperties.USERNAME);
        String password = writerConfig.getString(JDBCWriterProperties.PASSWORD);
        try {
            connection = JdbcUtils.getConnection(driver, url, username, password);
            connection.setAutoCommit(false);
        } catch (Exception e) {
            throw new HDataException("Failed to init JDBC connection.", e);
        }
    }

    private void prepareStatement(String sql) {
        LOG.debug(sql);
        try {
            statement = connection.prepareStatement(sql);
        } catch (Exception e) {
            throw new HDataException("Failed to prepare statement.", e);
        }
    }

    private String buildInsertSql(String table, List<String> columns, List<String> upsertColumns) {
        String[] placeholder = new String[columns.size()];
        Arrays.fill(placeholder, "?");
        String sql = String.format("INSERT INTO %s(%s) VALUES(%s)",
                table,
                keywordEscaper + Joiner.on(keywordEscaper + ", " + keywordEscaper).join(columns) + keywordEscaper,
                Joiner.on(", ").join(placeholder));
        // TODO: Upsert only support mysql for now
        return appendMysqlUpsertTail(sql, upsertColumns);
    }

    private String appendMysqlUpsertTail(String sql, List<String> upsertColumns) {
        if (upsertColumns == null || upsertColumns.isEmpty()) {
            return sql;
        }
        StringBuilder buf = new StringBuilder(sql);
        buf.append(" ON DUPLICATE KEY UPDATE ");
        for (int i = 0; i < upsertColumns.size(); i++) {
            if (i != 0) {
                buf.append(", ");
            }
            String col = upsertColumns.get(i);
            buf.append(keywordEscaper).append(col).append(keywordEscaper)
                    .append(" = VALUES(")
                    .append(keywordEscaper).append(col).append(keywordEscaper)
                    .append(")");
        }
        return buf.toString();
    }

    private String buildInsertSql(String table, int columnSize, List<String> upsertColumns) {
        String[] placeholder = new String[columnSize];
        Arrays.fill(placeholder, "?");
        String sql = String.format("INSERT INTO %s VALUES(%s)", table, Joiner.on(", ").join(placeholder));
        // TODO: Upsert only support mysql for now
        return appendMysqlUpsertTail(sql, upsertColumns);
    }

    @Override
    public void execute(Record record) {
        try {
            if (statement == null) {
                // TODO: statement must be prepared before execution
                prepareStatement(buildInsertSql(table, record.size(), this.upsertColumns));
            }

            for (int i = 0, len = record.size(); i < len; i++) {
                if (record.get(i) instanceof Timestamp && !Integer.valueOf(Types.TIMESTAMP).equals(columnTypes.get(columns.get(i).toLowerCase()))) {
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
