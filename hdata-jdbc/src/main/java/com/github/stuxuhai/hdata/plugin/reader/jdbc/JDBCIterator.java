package com.github.stuxuhai.hdata.plugin.reader.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCIterator {

	private static final Logger LOG = LogManager.getLogger(JDBCIterator.class);

	private List<JDBCUnit> unitList = new ArrayList<JDBCUnit>();
	private Integer length = 0;
	private Integer current = 0;

	public void add(JDBCUnit unit) {
		unitList.add(unit);
		length++;
	}

	/**
	 * 获取 下一条 SQL
	 *
	 */
	public synchronized String getNextSQL(int seq) {
		if (current >= length) {
			return null;
		}

		String sql = unitList.get(current).getNextSQL(seq);
		if (sql == null) {
			current++;
		} else {
			return sql;
		}

		return getNextSQL(seq);
	}

	protected static class JDBCUnit {
		private long startCursor;
		private long endCursor;
		private long start;
		private long end;
		private long step;
		private int parallelism;
		private int middle;
		private String column;
		private String sql;

		public JDBCUnit(String sql, String column, long start, long end, long step, int parallelism) {

			this.sql = sql;
			this.column = column;
			this.start = start;
			this.end = end;
			this.step = step;

			this.startCursor = start;
			this.endCursor = end;
			this.parallelism = parallelism;

			this.middle = (int) Math.ceil(parallelism / 2);
		}

		public String getNextSQL(int seq) {
			if (startCursor >= endCursor) {
				return null;
			}

			long tempStart, tempEnd;

			// from the start to the middle, from the end to the middle
			if (seq <= middle) {
				tempStart = startCursor;

				if (step <= 0 || startCursor + step > endCursor) {
					tempEnd = endCursor;
				} else {
					tempEnd = startCursor + step;
				}

				startCursor = tempEnd;
			} else {
				tempEnd = endCursor;

				if (step <= 0 || startCursor + step > endCursor) {
					tempStart = startCursor;
				} else {
					tempStart = endCursor - step;
				}

				endCursor = tempStart;
			}

			String currentSql = sql.replace(JDBCSplitter.CONDITIONS,
					column + " >= " + tempStart + " AND " + column + " < " + tempEnd);

			LOG.debug("sql:{}", currentSql);

			return currentSql;
		}

		@Override
		public String toString() {
			return "JDBCUnit{" + "startCursor=" + startCursor + ", endCursor=" + endCursor + ", start=" + start
					+ ", end=" + end + ", step=" + step + ", parallelism=" + parallelism + ", middle=" + middle
					+ ", column='" + column + '\'' + ", sql='" + sql + '\'' + '}';
		}
	}

}
