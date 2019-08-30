package org.apache.spark.sql.execution.datasources.druid;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.query.filter.DimFilter;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.realtime.firehose.IngestSegmentFirehose;
import io.druid.segment.realtime.firehose.WindowedStorageAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DruidRecordReader extends RecordReader<NullWritable, InputRow> {
    private static final Logger log = new Logger(DruidRecordReader.class);
    private QueryableIndex queryableIndex = null;
    private IngestSegmentFirehose firehose = null;
    private File tmpDir = null;
    private InputRow currentRow = null;

    @Override
    public void initialize(
            final InputSplit split,
            final TaskAttemptContext context
    ) throws IOException, InterruptedException {
        if (firehose != null) {
            firehose.close();
        }

        if (queryableIndex != null) {
            queryableIndex.close();
        }

        final WindowedDataSegment segment = ((DruidInputSplit) split).getSegment();

        queryableIndex = loadSegment(context, segment);
        firehose = makeFirehose(
                new WindowedStorageAdapter(
                        new QueryableIndexStorageAdapter(queryableIndex),
                        segment.getInterval()
                ),
                DruidInputFormat.getFilter(context.getConfiguration()),
                DruidInputFormat.getColumns(context.getConfiguration())
        );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (firehose.hasMore()) {
            currentRow = firehose.nextRow();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public InputRow getCurrentValue() throws IOException, InterruptedException {
        return currentRow;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (firehose != null) {
            firehose.close();
        }

        if (queryableIndex != null) {
            queryableIndex.close();
        }

        if (tmpDir != null) {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    private QueryableIndex loadSegment(
            final TaskAttemptContext context,
            final WindowedDataSegment segment
    ) throws IOException {
        if (tmpDir == null) {
            tmpDir = Files.createTempDir();
        }

        final Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));
        final File segmentDir = new File(tmpDir, segment.getSegment().getIdentifier());
        if (!segmentDir.exists()) {
            log.info("Fetching segment[%s] from[%s] to [%s].",
                    segment.getSegment().getIdentifier(), path, segmentDir);
            if (!segmentDir.mkdir()) {
                throw new ISE("Failed to make directory[%s]", segmentDir);
            }
            JobHelper.unzipNoGuava(path, context.getConfiguration(), segmentDir, context);
        }

//        File segmentDir = new File("/Users/zhangbin/Documents/0_index");
//        File segmentDir = new File("/Users/zhangbin/Documents/lineitem");
        final QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(segmentDir);
        log.info("Loaded segment[%s].", segment.getSegment().getIdentifier());
        return index;
    }

    private IngestSegmentFirehose makeFirehose(
            final WindowedStorageAdapter adapter,
            final DimFilter filter,
            final List<String> columns
    ) {
        // Split columns into dimensions and metrics.
        final List<String> dimensions = Lists.newArrayList();
        final List<String> metrics = Lists.newArrayList();

        if (columns == null) {
            Iterables.addAll(dimensions, adapter.getAdapter().getAvailableDimensions());
            Iterables.addAll(metrics, adapter.getAdapter().getAvailableMetrics());
        } else {
            final Set<String> availableDimensions =
                    Sets.newHashSet(adapter.getAdapter().getAvailableDimensions());
            final Set<String> availableMetrics =
                    Sets.newHashSet(adapter.getAdapter().getAvailableMetrics());
            for (String column : columns) {
                log.info("column:" + column);
                if (availableDimensions.contains(column)) {
                    dimensions.add(column);
                } else if (availableMetrics.contains(column)) {
                    metrics.add(column);
                }
            }
        }

        return new IngestSegmentFirehose(
                ImmutableList.of(adapter),
                dimensions,
                metrics,
                filter);
    }
}