package com.github.stuxuhai.hdata.plugin.hive;

import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveBaseChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import com.github.stuxuhai.hdata.api.Record;

public class HiveTypeUtils {

	/**
	 * 将Hive Writable类型转为标准Java类型
	 *
	 * @param o
	 * @return
	 */
	public static Object toJavaObject(Object o) {
		if (o instanceof HiveBaseChar) {
			return ((HiveVarchar) o).getValue();
		} else if (o instanceof HiveDecimal) {
			return ((HiveDecimal) o).bigDecimalValue();
		} else if (o instanceof BigInteger) {
			return ((BigInteger) o).longValue();
		}

		return o;
	}

	/**
	 * 将Hive Writable类型转为标准Java类型()
	 *
	 * @param type
	 * @param o
	 * @return Object
	 */
	public static Object toJavaObjectSpecial(String type, Object o) {
		if (type == null || o == null) {
			return toJavaObject(o);
		}

		// Hive中的bigint 对应long
		if (type.equals("bigint")) {
			if (o.toString().isEmpty()) {
				return 0L;
			}
			return Long.parseLong(o.toString());
		} else if (type.equals("int")) {
			if (o.toString().isEmpty()) {
				return 0;
			}
			return Integer.parseInt(o.toString());
		} else {
			return toJavaObject(o);
		}
	}

	/**
	 * 获取Hive类型的PrimitiveCategory
	 *
	 * @param type
	 * @return
	 */
	public static PrimitiveCategory getPrimitiveCategory(String type) {
		if ("TINYINT".equals(type)) {
			return PrimitiveObjectInspector.PrimitiveCategory.BYTE;
		} else if ("SMALLINT".equals(type)) {
			return PrimitiveObjectInspector.PrimitiveCategory.SHORT;
		} else if ("BIGINT".equals(type)) {
			return PrimitiveObjectInspector.PrimitiveCategory.LONG;
		} else {
			return PrimitiveObjectInspector.PrimitiveCategory.valueOf(type);
		}
	}

	/**
	 * converter special str in hive eg:NaN or Infinity to readable value in db
	 *
	 * @param record
	 * @param obj
	 * @param columnsTypes
	 * @return
	 */
	public static Record convertHiveSpecialValue(Record record, Object obj, String columnsTypes, boolean convertNull) {
		if (!convertNull && obj == null) {
			record.add(obj);
			return record;
		}

		boolean isSpecialStr = "NaN".equals(String.valueOf(obj)) || "Infinity".equals(String.valueOf(obj));
		if (convertNull) {
			isSpecialStr = isSpecialStr || obj == null;
		}

		if ("int".equals(columnsTypes)) {
			if (isSpecialStr) {
				record.add(Integer.valueOf(0));
			} else {
				record.add(Integer.valueOf(String.valueOf(obj)));
			}
		} else if ("bigint".equals(columnsTypes)) {
			if (isSpecialStr) {
				record.add(Long.valueOf(0));
			} else {
				record.add(Long.valueOf(String.valueOf(obj)));
			}
		} else if ("float".equals(columnsTypes)) {
			if (isSpecialStr) {
				record.add(Float.valueOf(0));
			} else {
				record.add(Float.valueOf(String.valueOf(obj)));
			}
		} else if ("double".equals(columnsTypes)) {
			if (isSpecialStr) {
				record.add(Double.valueOf(0));
			} else {
				record.add(Double.valueOf(String.valueOf(obj)));
			}
		} else {
			if (isSpecialStr) {
				record.add("");
			} else {
				record.add(obj);
			}
		}

		return record;

	}

}
