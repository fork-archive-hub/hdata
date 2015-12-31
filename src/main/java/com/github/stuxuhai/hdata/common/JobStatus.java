/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.common;

/**
 * @author wuya
 * 
 */
public enum JobStatus {

    SUCCESS(0), FAILED(1);

    private int status;

    JobStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }
}
