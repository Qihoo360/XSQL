package org.apache.spark.sql.execution.datasources.druid;

import com.google.common.base.Preconditions;
import com.metamx.common.logger.Logger;
import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DruidInputSplit extends InputSplit implements Writable {
    private String[] locations = new String[0];
    private WindowedDataSegment segment = null;
    private static final Logger log = new Logger(DruidInputSplit.class);

    public DruidInputSplit() {
    }

    public DruidInputSplit(String[] locations) {
        this.locations = locations;
    }

    public static DruidInputSplit create(WindowedDataSegment segment, Configuration conf) {
        String[] locations = setLocations(segment, conf);
        final DruidInputSplit split = new DruidInputSplit(locations);
        split.segment = Preconditions.checkNotNull(segment, "segment");
        return split;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return segment.getSegment().getSize();
    }

    private static String[] setLocations(WindowedDataSegment segment, Configuration conf) {
        final Path path = new Path(JobHelper.getURIFromSegment(segment.getSegment()));
        try {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(
                    status,
                    0,
                    status.getLen());
            return fileBlockLocations[0].getHosts();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    public WindowedDataSegment getSegment() {
        return segment;
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        final byte[] segmentBytes = DruidInputFormat.objectMapper().writeValueAsBytes(segment);
        out.writeInt(segmentBytes.length);
        out.write(segmentBytes);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        final int segmentBytesLength = in.readInt();
        final byte[] segmentBytes = new byte[segmentBytesLength];
        in.readFully(segmentBytes);
        this.segment = DruidInputFormat.objectMapper().readValue(segmentBytes,
                WindowedDataSegment.class);
    }
}
