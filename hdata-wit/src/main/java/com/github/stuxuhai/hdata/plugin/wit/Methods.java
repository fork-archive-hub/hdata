package com.github.stuxuhai.hdata.plugin.wit;

import com.github.stuxuhai.hdata.api.Record;
import org.febit.wit.Engine;
import org.febit.wit.core.NativeFactory;
import org.febit.wit.global.GlobalManager;
import org.febit.wit.util.JavaNativeUtil;

/**
 *
 * @author zqq90
 */
public class Methods implements WitEnginePlugin {

    public static final Record newRecord() {
        return new WitDynamicRecord();
    }

    public static final Record copyRecord(Record record) {
        return new WitDynamicRecord(record);
    }

    @Override
    public void handle(Engine engine) {
        NativeFactory nativeFactory = engine.getNativeFactory();
        GlobalManager manager = engine.getGlobalManager();
        JavaNativeUtil.addStaticMethods(manager, nativeFactory, Methods.class);
        JavaNativeUtil.addConstFields(manager, nativeFactory, Methods.class);
    }
}
