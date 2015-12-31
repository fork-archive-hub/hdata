/*
 * Author: wuya
 * Create Date: 2014年12月12日 上午9:20:27
 */
package com.github.stuxuhai.hdata.util;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * @author wuya
 *
 */
public class DataSourceUtils {

    private static Map<Integer, HikariDataSource> dataSourceMap = new ConcurrentHashMap<Integer, HikariDataSource>();

    public static synchronized HikariDataSource getDataSource(String driverClassName, String url, String username, String password) {
        HashFunction hashFunction = Hashing.md5();
        int hashcode = hashFunction.newHasher().putString(driverClassName, Charsets.UTF_8).putString(url, Charsets.UTF_8)
                .putString(username, Charsets.UTF_8).putString(password, Charsets.UTF_8).hash().asInt();

        HikariDataSource dataSource = dataSourceMap.get(hashcode);
        if (dataSource == null) {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(url);
            config.setDriverClassName(driverClassName);
            config.addDataSourceProperty("user", username);
            config.addDataSourceProperty("password", password);
            config.setMinimumIdle(1);
            config.setMaximumPoolSize(20);
            config.addDataSourceProperty("cachePrepStmts", "true");
            config.addDataSourceProperty("prepStmtCacheSize", "250");
            config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
            config.addDataSourceProperty("useServerPrepStmts", "true");

            dataSource = new HikariDataSource(config);
            dataSourceMap.put(hashcode, dataSource);
        }

        return dataSource;
    }

    public static synchronized void close() {
        for (Entry<Integer, HikariDataSource> entry : dataSourceMap.entrySet()) {
            HikariDataSource dataSource = entry.getValue();
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }
}
