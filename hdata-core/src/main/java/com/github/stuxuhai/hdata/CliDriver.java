package com.github.stuxuhai.hdata;

import com.github.stuxuhai.hdata.api.JobConfig;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;

import com.github.stuxuhai.hdata.api.PluginConfig;
import com.github.stuxuhai.hdata.config.DefaultJobConfig;
import com.github.stuxuhai.hdata.core.HData;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.google.common.base.Throwables;

public class CliDriver {

    private static final String XML_FILE_OPTION = "f";
    private static final String HDATA_VARS_OPTION = "D";
    private static final String QUIET_OPTION = "q";
    private static final String READER_OPTION = "reader";
    private static final String WRITER_OPTION = "writer";
    private static final String READER_VARS_OPTION = "R";
    private static final String WRITER_VARS_OPTION = "W";

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * 创建命令行选项
     * 
     * @return
     */
    public Options createOptions() {
        Options options = new Options();
        options.addOption(XML_FILE_OPTION, null, true, "job xml path");
        options.addOption(QUIET_OPTION, null, false, "quiet");
        options.addOption(Option.builder(HDATA_VARS_OPTION).hasArgs().build());

        options.addOption(null, READER_OPTION, true, "reader name");
        options.addOption(Option.builder(READER_VARS_OPTION).hasArgs().build());

        options.addOption(null, WRITER_OPTION, true, "writer name");
        options.addOption(Option.builder(WRITER_VARS_OPTION).hasArgs().build());
        return options;
    }

    /**
     * 打印命令行帮助信息
     * 
     * @param options
     */
    public void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(" ", options);
    }

    /**
     * 替换命令行变量
     * 
     * @param config
     * @param vars
     */
    public void replaceConfigVars(PluginConfig config, Properties vars) {
        for (Entry<Object, Object> confEntry : config.entrySet()) {
            if (confEntry.getKey().getClass() == String.class && confEntry.getValue().getClass() == String.class) {
                for (Entry<Object, Object> varEntry : vars.entrySet()) {
                    String replaceVar = "${" + varEntry.getKey() + "}";
                    if (confEntry.getValue().toString().contains(replaceVar)) {
                        config.put(confEntry.getKey(), confEntry.getValue().toString().replace(replaceVar, varEntry.getValue().toString()));
                    }
                }
            }
        }
    }

    private void putOptionValues(Properties props, String[] values) {
        if (props != null && values != null) {
            for (int i = 0, len = values.length; i < len; i++) {
                props.put(values[i], values[++i]);
            }
        }
    }

    /**
     * 主程序入口
     * 
     * @param args
     */
    public static void main(String[] args) {

        CliDriver cliDriver = new CliDriver();
        Options options = cliDriver.createOptions();
        if (args.length < 1) {
            cliDriver.printHelp(options);
            System.exit(-1);
        }

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption(QUIET_OPTION)) {
                LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
                Configuration conf = ctx.getConfiguration();
                conf.getLoggerConfig(LogManager.ROOT_LOGGER_NAME).setLevel(Level.WARN);
                ctx.updateLoggers(conf);
            }

            final JobConfig jobConfig;
            if (cmd.hasOption(XML_FILE_OPTION)) {
                String jobXmlPath = cmd.getOptionValue(XML_FILE_OPTION);
                jobConfig = DefaultJobConfig.createFromXML(jobXmlPath);
                Properties vars = new Properties();
                cliDriver.putOptionValues(vars, cmd.getOptionValues(HDATA_VARS_OPTION));

                final PluginConfig readerConfig = jobConfig.getReaderConfig();
                final PluginConfig writerConfig = jobConfig.getWriterConfig();

                cliDriver.replaceConfigVars(readerConfig, vars);
                cliDriver.replaceConfigVars(writerConfig, vars);
            } else {
                if (!cmd.hasOption(READER_OPTION) || !cmd.hasOption(WRITER_OPTION)) {
                    throw new HDataException("Option --reader and --writer should be both given if -f option not exists.");
                }

                String readerName = cmd.getOptionValue(READER_OPTION);
                String writerName = cmd.getOptionValue(WRITER_OPTION);

                PluginConfig readerConfig = new PluginConfig();
                cliDriver.putOptionValues(readerConfig, cmd.getOptionValues(READER_VARS_OPTION));

                PluginConfig writerConfig = new PluginConfig();
                cliDriver.putOptionValues(writerConfig, cmd.getOptionValues(WRITER_VARS_OPTION));

                jobConfig = new DefaultJobConfig(readerName, readerConfig, writerName, writerConfig);
            }

            HData hData = new HData();
            hData.start(jobConfig);
        } catch (ParseException e) {
            cliDriver.printHelp(options);
            System.exit(-1);
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
            System.exit(-1);
        }
    }
}
