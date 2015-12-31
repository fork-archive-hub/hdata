/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.tool;

import java.sql.Connection;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbutils.DbUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.stuxuhai.hdata.util.DataSourceUtils;
import com.google.common.base.Throwables;

public class SQLExecuteTool {

    private static final String JDBC_DRIVER = "jdbc-driver";
    private static final String JDBC_URL = "jdbc-url";
    private static final String JDBC_USERNAME = "jdbc-username";
    private static final String JDBC_PASSWORD = "jdbc-password";
    private static final String SQL = "sql";
    private static final Logger LOGGER = LogManager.getLogger(SQLExecuteTool.class);

    public Options createOptions() {
        Options options = new Options();
        options.addOption(null, JDBC_DRIVER, true, "jdbc driver class name");
        options.addOption(null, JDBC_URL, true, "jdbc url, e.g., jdbc:mysql://localhost:3306/database");
        options.addOption(null, JDBC_USERNAME, true, "jdbc username");
        options.addOption(null, JDBC_PASSWORD, true, "jdbc password");
        options.addOption(null, SQL, true, "sql");
        return options;
    }

    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    public static void main(String[] args) {
        SQLExecuteTool tool = new SQLExecuteTool();
        Options options = tool.createOptions();
        if (args.length < 1) {
            tool.printHelp(options);
            System.exit(-1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        Connection conn = null;
        try {
            cmd = parser.parse(options, args);
            String driver = cmd.getOptionValue(JDBC_DRIVER);
            String url = cmd.getOptionValue(JDBC_URL);
            String username = cmd.getOptionValue(JDBC_USERNAME);
            String password = cmd.getOptionValue(JDBC_PASSWORD);
            String sql = cmd.getOptionValue(SQL);
            conn = DataSourceUtils.getDataSource(driver, url, username, password).getConnection();
            Statement statement = conn.createStatement();

            LOGGER.info("Executing sql: {}", sql);
            statement.execute(sql);
            LOGGER.info("Execute successfully.");
        } catch (ParseException e) {
            tool.printHelp(options);
            System.exit(-1);
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            System.exit(-1);
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }

}
