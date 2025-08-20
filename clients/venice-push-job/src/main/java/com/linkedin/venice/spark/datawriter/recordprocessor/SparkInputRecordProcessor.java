package com.linkedin.venice.spark.datawriter.recordprocessor;

import com.linkedin.venice.hadoop.input.recordreader.AbstractVeniceRecordReader;
import com.linkedin.venice.hadoop.input.recordreader.avro.IdentityVeniceRecordReader;
import com.linkedin.venice.hadoop.task.datawriter.AbstractInputRecordProcessor;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.spark.engine.SparkEngineTaskConfigProvider;
import com.linkedin.venice.utils.TriConsumer;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;


/**
 * An implementation of {@link AbstractInputRecordProcessor} for Spark that processes input records from the dataframe
 * and emits an {@link Iterator} of {@link Row} with {@link SparkConstants#DEFAULT_SCHEMA} as the schema.
 */
public class SparkInputRecordProcessor extends AbstractInputRecordProcessor<Row> {
  private final DataWriterTaskTracker dataWriterTaskTracker;

  public SparkInputRecordProcessor(Properties jobProperties, DataWriterAccumulators accumulators) {
    dataWriterTaskTracker = new SparkDataWriterTaskTracker(accumulators);
    super.configure(new SparkEngineTaskConfigProvider(jobProperties));
  }

  public Iterator<Row> processRecord(Row record) {
    List<Row> outputRows = new ArrayList<>();
    super.processRecord(record, getRecordEmitter(outputRows), dataWriterTaskTracker);
    return outputRows.iterator();
  }

  @Override
  protected AbstractVeniceRecordReader<Row> getRecordReader(VeniceProperties props) {
    return IdentityVeniceRecordReader.getInstance();
  }

  private TriConsumer<byte[], byte[], Long> getRecordEmitter(List<Row> rows) {
    return (key, value, timestamp) -> {
      rows.add(new GenericRowWithSchema(new Object[] { key, value, timestamp }, SparkConstants.DEFAULT_SCHEMA));
    };
  }
}
