/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.core;

import java.util.ArrayList;
import java.util.Collections;

public class Fields extends ArrayList<String> {

    private static final long serialVersionUID = -174064216143075549L;

    public Fields() {
        super();
    }

    public Fields(String... fields) {
        super();
        Collections.addAll(this, fields);
    }

}
