/*
 * Author: wuya
 * Create Date: 2014年6月26日 下午4:35:16
 */
package com.github.stuxuhai.hdata.plugin.writer.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.HadoopShims.HCatHadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.transfer.WriteEntity;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

import com.github.stuxuhai.hdata.common.Constants;
import com.github.stuxuhai.hdata.config.PluginConfig;
import com.github.stuxuhai.hdata.core.JobContext;
import com.github.stuxuhai.hdata.exception.HDataException;
import com.github.stuxuhai.hdata.plugin.Record;
import com.github.stuxuhai.hdata.plugin.Writer;
import com.github.stuxuhai.hdata.util.HiveTypeUtils;
import com.google.common.base.Preconditions;

public class HiveWriter extends Writer {

    private TaskAttemptContext taskAttemptContext;
    private RecordWriter<WritableComparable<?>, HCatRecord> writer;
    private OutputCommitter committer;
    private static AtomicInteger sequence = new AtomicInteger(0);
    private List<String> columns = new ArrayList<String>();

    @Override
    public void prepare(JobContext context, PluginConfig writerConfig) {
        String metastoreUris = writerConfig.getString(HiveWriterProperties.METASTORE_URIS);
        Preconditions.checkNotNull(metastoreUris, "Hive writer requires property: metastore.uris");

        String hiveDatabase = writerConfig.getString(HiveWriterProperties.DATABASE, "default");
        String hiveTable = writerConfig.getString(HiveWriterProperties.TABLE);
        Preconditions.checkNotNull(hiveTable, "Hive writer requires property: table");

        if (writerConfig.containsKey(HiveWriterProperties.HADOOP_USER)) {
            System.setProperty("HADOOP_USER_NAME", writerConfig.getString(HiveWriterProperties.HADOOP_USER));
        }

        Map<String, String> config = new HashMap<String, String>();
        config.put(ConfVars.METASTOREURIS.varname, metastoreUris);
        config.put("hive.start.cleanup.scratchdir", "true");

        WriteEntity.Builder builder = new WriteEntity.Builder();
        WriteEntity entity;

        if (writerConfig.containsKey(HiveWriterProperties.PARTITIONS) || System.getProperty(HiveWriterProperties.PARTITIONS) != null) {
            Map<String, String> partition = new HashMap<String, String>();
            String partitions = writerConfig.getString(HiveWriterProperties.PARTITIONS) != null
                    ? writerConfig.getString(HiveWriterProperties.PARTITIONS) : System.getProperty(HiveWriterProperties.PARTITIONS);
            //            String[] partkvs = writerConfig.getString(HiveWriterProperties.PARTITIONS).split(Constants.COLUMNS_SPLIT_REGEX);
            String[] partkvs = partitions.split(Constants.COLUMNS_SPLIT_REGEX);
            for (String kvs : partkvs) {
                String[] tokens = kvs.split("=", 2);
                partition.put(tokens[0], tokens[1]);
            }

            entity = builder.withDatabase(hiveDatabase).withTable(hiveTable).withPartition(partition).build();
        } else {
            entity = builder.withDatabase(hiveDatabase).withTable(hiveTable).build();
        }

        Configuration conf = new Configuration();
        if (writerConfig.containsKey(HiveWriterProperties.HDFS_CONF_PATH)) {
            conf.addResource(new Path("file://" + writerConfig.getString(HiveWriterProperties.HDFS_CONF_PATH)));
        }
        int id = (int) (System.currentTimeMillis() + sequence.getAndAdd(1));
        if (id < 0) {
            id = -id;
        }
        conf.setInt("mapred.task.partition", id);
        for (Entry<String, String> entry : config.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }

        OutputJobInfo jobInfo = OutputJobInfo.create(entity.getDbName(), entity.getTableName(), entity.getPartitionKVs());
        try {
            Job job = Job.getInstance(conf);
            HCatOutputFormat.setOutput(job, jobInfo);
            HCatOutputFormat.setSchema(job, HCatOutputFormat.getTableSchema(job.getConfiguration()));

            HCatOutputFormat outputFormat = new HCatOutputFormat();

            List<HCatFieldSchema> list = HCatOutputFormat.getTableSchema(job.getConfiguration()).getFields();
            for (HCatFieldSchema schema : list) {
                columns.add(schema.getTypeString());
            }

            HCatHadoopShims hCatHadoopShims = ShimLoader.getHadoopShims().getHCatShim();
            TaskAttemptID taskAttemptID = TaskAttemptID.forName("attempt_" + id + "_" + id + "_m_" + id + "_0");
            job.getConfiguration().set("mapreduce.task.attempt.id", taskAttemptID.toString());

            taskAttemptContext = hCatHadoopShims.createTaskAttemptContext(job.getConfiguration(), taskAttemptID);
            committer = outputFormat.getOutputCommitter(taskAttemptContext);
            committer.setupTask(taskAttemptContext);

            writer = outputFormat.getRecordWriter(taskAttemptContext);
        } catch (IOException e) {
            throw new HDataException(e);
        } catch (InterruptedException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void execute(Record record) {
        HCatRecord hCatRecord = new DefaultHCatRecord(record.size());
        int columnSize = columns.size();
        for (int i = 0, len = record.size(); i < len; i++) {
            Object item = record.get(i);
            if (columnSize > i) {
                hCatRecord.set(i, HiveTypeUtils.toJavaObjectSpecial(columns.get(i), item));
            } else {
                hCatRecord.set(i, HiveTypeUtils.toJavaObjectSpecial(null, item));
            }
        }

        try {
            writer.write(null, hCatRecord);
        } catch (IOException e) {
            throw new HDataException(e);
        } catch (InterruptedException e) {
            throw new HDataException(e);
        }
    }

    @Override
    public void close() {
        if (writer != null) {
            try {
                writer.close(taskAttemptContext);
                if (committer.needsTaskCommit(taskAttemptContext)) {
                    committer.commitTask(taskAttemptContext);
                }

                committer.commitJob(taskAttemptContext);
            } catch (IOException e) {
                throw new HDataException(e);
            } catch (InterruptedException e) {
                throw new HDataException(e);
            }
        }
    }
}
