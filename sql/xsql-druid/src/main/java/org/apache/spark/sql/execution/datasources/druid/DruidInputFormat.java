package org.apache.spark.sql.execution.datasources.druid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.net.HostAndPort;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.query.filter.DimFilter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;

import java.io.IOException;
import java.util.List;

public class DruidInputFormat extends InputFormat<NullWritable, InputRow> {
    public static final String CONF_COORDINATOR_HOST = "druid.inputformat.coordinator.host";
    public static final String CONF_DATASOURCE = "druid.inputformat.dataSource";
    public static final String CONF_INTERVALS = "druid.inputformat.intervals";
    public static final String CONF_FILTER = "druid.inputformat.filter";
    public static final String CONF_COLUMNS = "druid.inputformat.columns";

    private static final Logger log = new Logger(DruidInputFormat.class);
    private static final Interval ETERNITY = new Interval(
        new DateTime("0000", ISOChronology.getInstanceUTC()),
        new DateTime("9000", ISOChronology.getInstanceUTC())
    );

    public static void setInputs(
        final Configuration conf,
        final String coordinatorHost,
        final String dataSource,
        final List<Interval> intervals,
        final DimFilter filter,
        final List<String> columns
    ) throws IOException {
        conf.set(CONF_COORDINATOR_HOST,
                Preconditions.checkNotNull(coordinatorHost, "coordinatorHost"));
        conf.set(CONF_DATASOURCE,
                Preconditions.checkNotNull(dataSource, "dataSource"));

        if (intervals != null) {
            conf.set(CONF_INTERVALS, objectMapper().writeValueAsString(intervals));
        }

        if (filter != null) {
            conf.set(CONF_FILTER, objectMapper().writeValueAsString(filter));
        }

        if (columns != null) {
            conf.set(CONF_COLUMNS, objectMapper().writeValueAsString(columns));
        }
    }

    static ObjectMapper objectMapper()
    {
        return HadoopDruidIndexerConfig.JSON_MAPPER;
    }

    public static HostAndPort getCoordinatorHost(final Configuration conf) {
        final String s = Preconditions.checkNotNull(conf.get(CONF_COORDINATOR_HOST),
                                                    CONF_COORDINATOR_HOST);
        return HostAndPort.fromString(s);
    }

    public static String getDataSource(final Configuration conf) {
        return Preconditions.checkNotNull(conf.get(CONF_DATASOURCE), CONF_DATASOURCE);
    }

    public static List<Interval> getIntervals(final Configuration conf) {
        final String s = conf.get(CONF_INTERVALS);
        try {
            if (s != null) {
                return objectMapper().readValue(s, new TypeReference<List<Interval>>() {
                });
            } else {
                return ImmutableList.of(ETERNITY);
            }
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static DimFilter getFilter(final Configuration conf) {
        final String s = conf.get(CONF_FILTER);
        if (s == null) {
            return null;
        }
        try {
            return objectMapper().readValue(s, DimFilter.class);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public static List<String> getColumns(final Configuration conf) {
        final String s = conf.get(CONF_COLUMNS);
        if (s == null) {
            return null;
        }
        try {
            return objectMapper().readValue(s, new TypeReference<List<String>>()
            {
            });
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext)
            throws IOException, InterruptedException {
        final Configuration conf = jobContext.getConfiguration();
        final String dataSource = getDataSource(conf);
        final List<Interval> intervals = getIntervals(conf);
        final List<DataSegment> segments;
        try (final HttpClientHolder httpClient = HttpClientHolder.create()) {
            segments = new DruidMetadataClient(
                    httpClient.get(),
                    objectMapper(),
                    getCoordinatorHost(conf)
            ).usedSegments(dataSource, intervals);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        log.info(
                "Got %,d used segments for dataSource[%s], intervals[%s] from coordinator.",
                segments.size(),
                dataSource,
                Joiner.on(", ").join(intervals)
        );

        // Window the DataSegments by putting them in a timeline.
        final VersionedIntervalTimeline<String, DataSegment> timeline =
                new VersionedIntervalTimeline<>(Ordering.natural());
        for (DataSegment segment : segments) {
            timeline.add(
                    segment.getInterval(),
                    segment.getVersion(),
                    segment.getShardSpec().createChunk(segment));
        }

        final List<InputSplit> splits = Lists.newArrayList();

        for (Interval interval : intervals) {
            final List<TimelineObjectHolder<String, DataSegment>> lookup =
                    timeline.lookup(interval);
            for (final TimelineObjectHolder<String, DataSegment> holder : lookup) {
                for (final PartitionChunk<DataSegment> chunk : holder.getObject()) {
                    final WindowedDataSegment windowedDataSegment = new WindowedDataSegment(
                            chunk.getObject(),
                            holder.getInterval()
                    );
                    splits.add(DruidInputSplit.create(windowedDataSegment, conf));
                }
            }
        }

        log.info(
            "Found %,d splits for dataSource[%s], intervals[%s].",
            splits.size(),
            dataSource,
            Joiner.on(", ").join(intervals)
        );

        return splits;
    }

    @Override
    public RecordReader<NullWritable, InputRow> createRecordReader(
        final InputSplit split,
        final TaskAttemptContext context
    ) throws IOException, InterruptedException {
        return new DruidRecordReader();
    }
}