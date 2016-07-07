/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author: xingtian
 * Create Date: 2015年1月16日 下午3:35:16
 */
package com.github.stuxuhai.hdata.util;

/**
 * 数字处理工具类
 *
 * */
public class NumberUtils {
	/**
	 * 获取 起始和结束 范围内的所有 数值
	 * 
	 * */
	public static int[] getRange(int before, int after) {
		int bigger = Math.max(before, after);
		int smaller = Math.min(before, after);

		int[] range = new int[bigger + 1 - smaller];
		for (int i = smaller; i <= bigger; i++) {
			range[i - smaller] = i;
		}

		return range;
	}

}
