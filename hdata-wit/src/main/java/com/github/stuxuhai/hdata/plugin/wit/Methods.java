package com.github.stuxuhai.hdata.plugin.wit;

import com.github.stuxuhai.hdata.api.Record;
import org.febit.wit.Context;
import org.febit.wit.Engine;
import org.febit.wit.InternalContext;
import org.febit.wit.core.NativeFactory;
import org.febit.wit.global.GlobalManager;
import org.febit.wit.lang.MethodDeclare;
import org.febit.wit.plugin.EnginePlugin;
import org.febit.wit.util.JavaNativeUtil;

/**
 *
 * @author zqq90
 */
public class Methods implements EnginePlugin {

    /**
     * A empty function, do nothing.
     */
    public static final MethodDeclare noop = new MethodDeclare() {
        @Override
        public Object invoke(InternalContext context, Object[] args) {
            return Context.VOID;
        }
    };

    public static final Record newRecord() {
        return new WitDynamicRecord();
    }

    public static final Record copyRecord(Record record) {
        return new WitDynamicRecord(record);
    }

    @Override
    public void apply(Engine engine) {
        NativeFactory nativeFactory = engine.getNativeFactory();
        GlobalManager manager = engine.getGlobalManager();
        JavaNativeUtil.addStaticMethods(manager, nativeFactory, Methods.class);
        JavaNativeUtil.addConstFields(manager, nativeFactory, Methods.class);
    }
}
