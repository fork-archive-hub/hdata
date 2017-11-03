package com.github.stuxuhai.hdata.plugin.wit.resolvers;

import com.github.stuxuhai.hdata.api.Record;
import org.febit.wit.exceptions.ScriptRuntimeException;
import org.febit.wit.resolvers.GetResolver;
import org.febit.wit.resolvers.SetResolver;
import org.febit.wit.util.StringUtil;

/**
 *
 * @author zqq90
 */
public class RecordResolver implements GetResolver, SetResolver {

    @Override
    public Class getMatchClass() {
        return Record.class;
    }

    @Override
    public Object get(Object object, Object property) {
        if (property instanceof Number) {
            try {
                return ((Record) object).get(((Number) property).intValue());
            } catch (IndexOutOfBoundsException e) {
                throw new ScriptRuntimeException(StringUtil.format("index out of bounds:{}", property));
            }
        }
        switch (property.toString()) {
            case "size":
            case "length":
                return ((Record) object).size();
        }
        throw new ScriptRuntimeException(StringUtil.format("Invalid property or can't read: com.github.stuxuhai.hdata.api.Record#{}", property));
    }

    @Override
    public void set(Object object, Object property, Object value) {
        if (property instanceof Number) {
            ((Record) object).add(((Number) property).intValue(), value);
            return;
        }
        throw new ScriptRuntimeException(StringUtil.format("Invalid property or can't write: com.github.stuxuhai.hdata.api.Record#{}", property));
    }
}
