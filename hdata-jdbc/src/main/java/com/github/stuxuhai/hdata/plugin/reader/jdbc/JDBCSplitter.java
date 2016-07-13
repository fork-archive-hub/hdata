package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbutils.DbUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.api.JobConfig;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Splitter;
import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.util.JdbcUtils;
import com.github.stuxuhai.hdata.util.NumberUtils;
import com.github.stuxuhai.hdata.util.Utils;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class JDBCSplitter extends Splitter {

    public static final String CONDITIONS = "$CONDITIONS";
    private static final Pattern PATTERN = Pattern.compile("^([a-zA-Z]\\w*)(\\[(\\d+)-(\\d+)\\])?(_(\\d+)_(\\d+)_(\\d+))?$");

    private static final Logger LOG = LogManager.getLogger(JDBCSplitter.class);

    private void checkIfContainsConditionKey(String sql, String errorMessage) {
        if (!sql.contains(CONDITIONS)) {
            LOG.error(errorMessage);
        }
    }

    private List<PluginConfig> buildPluginConfigs(Connection conn, List<String> sqlList, String splitColumn, PluginConfig readerConfig) {
        List<PluginConfig> list = new ArrayList<PluginConfig>();
        try {
            int parallelism = readerConfig.getParallelism();
            int maxFetchSize = readerConfig.getInt(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 0);
            JDBCIterator iterator = new JDBCIterator();

            for (String sql : sqlList) {
                double[] minAndMax = JdbcUtils.querySplitColumnRange(conn, sql.replace(CONDITIONS, "(1 = 1)"), splitColumn);
                double min = minAndMax[0];
                double max = minAndMax[1] + 1;

                iterator.add(new JDBCIterator.JDBCUnit(sql, splitColumn, (long) min, (long) max, maxFetchSize, parallelism));

            }

            for (int i = 0; i < parallelism; i++) {
                PluginConfig otherReaderConfig = (PluginConfig) readerConfig.clone();
                otherReaderConfig.put(JDBCReaderProperties.SQL_ITERATOR, iterator);
                otherReaderConfig.put(JDBCReaderProperties.SQL_SEQ, i);
                list.add(otherReaderConfig);
            }
            return list;
        } catch (SQLException e) {
            throw new HDataException(e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }

    @Override
    public List<PluginConfig> split(JobConfig jobConfig) {
        PluginConfig readerConfig = jobConfig.getReaderConfig();
        String keywordEscaper = readerConfig.getProperty(JDBCReaderProperties.KEYWORD_ESCAPER, "`");
        String driver = readerConfig.getString(JDBCReaderProperties.DRIVER);
        Preconditions.checkNotNull(driver, "JDBC reader required property: driver");

        String url = readerConfig.getString(JDBCReaderProperties.URL);
        Preconditions.checkNotNull(url, "JDBC reader required property: url");

        String username = readerConfig.getString(JDBCReaderProperties.USERNAME);
        String password = readerConfig.getString(JDBCReaderProperties.PASSWORD);
        int parallelism = readerConfig.getParallelism();
        int maxFetchSize = readerConfig.getInt(JDBCReaderProperties.MAX_SIZE_PER_FETCH, 0);

        List<String> sqlList = new ArrayList<String>();

        if (readerConfig.containsKey(JDBCReaderProperties.SQL)) {
            if (parallelism > 1) {
                checkIfContainsConditionKey(readerConfig.getString(JDBCReaderProperties.SQL),
                        "Reader must contains key word \"" + CONDITIONS + "\" in sql property when parallelism > 1.");
            }
            sqlList.add(readerConfig.getString(JDBCReaderProperties.SQL));
        } else {
            String table = readerConfig.getString(JDBCReaderProperties.TABLE);

            Preconditions.checkNotNull(table, "JDBC reader required property: table");

            if (!isMatch(table)) {
                throw new HDataException("table:" + table + " 格式错误");
            }

            List<String> tableList = getRange(table);

            for (String tableName : tableList) {
                StringBuilder sql = new StringBuilder();
                sql.append("SELECT ");
                if (!readerConfig.containsKey(JDBCReaderProperties.COLUMNS) && !readerConfig.containsKey(JDBCReaderProperties.EXCLUDE_COLUMNS)) {
                    sql.append("*");
                } else if (readerConfig.containsKey(JDBCReaderProperties.COLUMNS)) {
                    String columns = readerConfig.getString(JDBCReaderProperties.COLUMNS);
                    sql.append(columns);
                } else if (readerConfig.containsKey(JDBCReaderProperties.EXCLUDE_COLUMNS)) {
                    String[] excludeColumns = readerConfig.getString(JDBCReaderProperties.EXCLUDE_COLUMNS).trim()
                            .split(Constants.COLUMNS_SPLIT_REGEX);
                    Connection conn;
                    try {
                        conn = JdbcUtils.getConnection(driver, url, username, password);
                        String selectColumns = keywordEscaper + Joiner.on(keywordEscaper + ", " + keywordEscaper)
                                .join(Utils.getColumns(JdbcUtils.getColumnNames(conn, tableName, keywordEscaper), excludeColumns)) + keywordEscaper;
                        sql.append(selectColumns);
                    } catch (Exception e) {
                        throw new HDataException(e);
                    }

                }
                sql.append(" FROM ");
                sql.append(keywordEscaper).append(tableName).append(keywordEscaper);

                if (readerConfig.containsKey(JDBCReaderProperties.WHERE)) {
                    String where = readerConfig.getString(JDBCReaderProperties.WHERE);
                    sql.append(" WHERE ");
                    sql.append(where);
                    sql.append(" AND $CONDITIONS");
                } else if (readerConfig.containsKey(JDBCReaderProperties.SPLIT_BY) || parallelism > 1 || maxFetchSize > 0) {
                    sql.append(" WHERE $CONDITIONS");
                }

                sqlList.add(sql.toString());

            }
        }

        readerConfig.put(JDBCReaderProperties.SQL, sqlList);

        if (readerConfig.containsKey(JDBCReaderProperties.SPLIT_BY)) {
            String splitColumn = readerConfig.getString(JDBCReaderProperties.SPLIT_BY);
            LOG.debug("Get split-by column: {}", splitColumn);

            Connection conn = null;
            try {
                conn = JdbcUtils.getConnection(driver, url, username, password);
                return buildPluginConfigs(conn, sqlList, splitColumn, readerConfig);
            } catch (Exception e) {
                throw new HDataException(e);
            } finally {
                DbUtils.closeQuietly(conn);
            }
        } else {
            if (parallelism > 1 || maxFetchSize > 0) {
                Connection conn = null;
                try {
                    String table = readerConfig.getString(JDBCReaderProperties.TABLE);
                    LOG.info("Attempt to query digital primary key for table: {}", table);
                    conn = JdbcUtils.getConnection(driver, url, username, password);
                    String splitColumn = JdbcUtils.getDigitalPrimaryKey(conn, conn.getCatalog(), null, getFirstTableName(table), keywordEscaper);
                    if (splitColumn != null) {
                        LOG.info("Table {} find digital primary key: {}", table, splitColumn);
                        return buildPluginConfigs(conn, sqlList, keywordEscaper + splitColumn + keywordEscaper, readerConfig);
                    } else {
                        LOG.info("Table {} can not find digital primary key.", table);
                    }
                } catch (Exception e) {
                    throw new HDataException(e);
                } finally {
                    DbUtils.closeQuietly(conn);
                }
            }

            if (parallelism > 1) {
                LOG.warn(
                        "Reader parallelism is set to {}, but the \"split-by\" config is not given, so reader parallelism is set to default value: 1.",
                        parallelism);
            }

            List<PluginConfig> readerConfigList = new ArrayList<PluginConfig>();
            for (int i = 0; i < sqlList.size(); i++) {
                sqlList.set(i, sqlList.get(i).replace(CONDITIONS, "(1 = 1)"));
            }

            readerConfigList.add(readerConfig);
            return readerConfigList;
        }
    }

    /**
     * 表名是否 符合要求
     *
     */
    public static Boolean isMatch(String content) {
        for (String piece : com.google.common.base.Splitter.on(",").omitEmptyStrings().trimResults().split(content)) {
            if (!PATTERN.matcher(piece).find()) {
                return false;
            }
        }

        return true;
    }

    /**
     * 内容解析成列表
     *
     */
    public static List<String> getRange(String content) {
        // split to pieces and be unique.
        HashSet<String> hs = new HashSet<String>();
        for (String piece : com.google.common.base.Splitter.on(",").omitEmptyStrings().trimResults().split(content)) {
            hs.addAll(parseRange(piece));
        }

        List<String> range = new ArrayList<String>(hs);

        Collections.sort(range);

        return range;
    }

    /**
     * get the range
     *
     * 01-04 = 01,02,03,04
     *
     */
    private static List<String> parseRange(String content) {
        Matcher matcher = PATTERN.matcher(content);
        List<String> pieces = new ArrayList<String>();

        if (!matcher.find()) {
            throw new RuntimeException(content + ": The format is wrong.");
        }

        if (!content.contains("[")) {
            pieces.add(content);
            return pieces;
        }

        String prefix = matcher.group(1);
        String begin = matcher.group(3);
        String after = matcher.group(4);

        String format = "%0" + begin.length() + "d";

        int[] rangeList = NumberUtils.getRange(Integer.valueOf(begin), Integer.valueOf(after));

        for (int number : rangeList) {
            String suffix = matcher.group(5);
            if (suffix != null) {
                pieces.add(prefix + String.format(format, number) + suffix);
            } else {
                pieces.add(prefix + String.format(format, number));
            }
        }

        return pieces;
    }

    public static String getFirstTableName(String tableName) {
        return getRange(tableName).get(0);
    }

}
