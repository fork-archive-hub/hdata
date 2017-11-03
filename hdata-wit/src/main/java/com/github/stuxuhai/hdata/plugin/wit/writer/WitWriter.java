package com.github.stuxuhai.hdata.plugin.wit.writer;

import java.io.IOException;
import com.github.stuxuhai.hdata.api.JobContext;
import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.api.Record;
import com.github.stuxuhai.hdata.api.Writer;
import com.github.stuxuhai.hdata.core.PluginLoader;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.wit.WitEnginePlugin;
import com.github.stuxuhai.hdata.plugin.wit.WitRecord;
import com.github.stuxuhai.hdata.util.PluginUtils;
import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.ServiceLoader;
import org.febit.wit.Engine;
import org.febit.wit.Template;
import org.febit.wit.io.impl.DiscardOut;
import org.febit.wit.util.KeyValuesUtil;

public class WitWriter extends Writer {

    public static final String KEY_INPUT = "input";
    public static final String KEY_RESULT = "result";

    protected static final DiscardOut DISCARD_OUT = new DiscardOut();

    private static class LazyHolder {

        static final Engine ENGINE;

        static {
            ENGINE = Engine.create("hdata-wit-writer.wim");
            Iterator<WitEnginePlugin> iterator = ServiceLoader.load(WitEnginePlugin.class).iterator();
            while (iterator.hasNext()) {
                iterator.next().handle(ENGINE);
            }
        }
    }

    private Template template = null;
    private Writer innerWriter = null;
    private final String[] templateParamNames = new String[]{KEY_INPUT, KEY_RESULT};

    protected Template createTemplate(String tmpl) throws IOException {
        return LazyHolder.ENGINE.getTemplate("code: var input,result; (()->{\n" + tmpl + "\n})();");
    }

    protected WitRecord executeTemplate(Record input) {
        WitRecord result = new WitRecord();
        template.merge(KeyValuesUtil.wrap(templateParamNames, new Object[]{input, result}), DISCARD_OUT);
        return result;
    }

    protected Writer createInnerWriter(JobContext context, PluginConfig writerConfig) {
        String innerWriterName = writerConfig.getString(WitWriterProperties.INNER_WRITER);
        Preconditions.checkNotNull(innerWriterName, "Wit writer required property: " + WitWriterProperties.INNER_WRITER);

        String writerClassName = PluginLoader.getWriterClassName(innerWriterName);
        Preconditions.checkNotNull(writerClassName, "Can not find class for writer: " + innerWriterName);

        try {
            return (Writer) PluginUtils.loadClass(innerWriterName, writerClassName).newInstance();
        } catch (Exception e) {
            throw new HDataException("Can not create new writer instance for: " + innerWriterName, e);
        }
    }

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        String wit = writerConfig.getString(WitWriterProperties.WIT);
        Preconditions.checkNotNull(wit, "Wit writer required property: " + WitWriterProperties.WIT);

        try {
            this.template = createTemplate(wit);
        } catch (IOException ex) {
            throw new HDataException("Failed to load wit", ex);
        }
        this.template.reload();

        PluginConfig innerWriterConfig = new PluginConfig();
        innerWriterConfig.putAll(writerConfig);
        innerWriterConfig.remove(WitWriterProperties.INNER_WRITER);
        innerWriterConfig.remove(WitWriterProperties.WIT);
        this.innerWriter = createInnerWriter(context, writerConfig);
        this.innerWriter.prepare(context, innerWriterConfig);
    }

    @Override
    public void execute(Record input) {
        WitRecord result = executeTemplate(input);
        this.innerWriter.execute(result);
    }

    @Override
    public void close() {
        this.innerWriter.close();
    }

}
