/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.druid;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidSplit extends InputSplit implements Writable {

    private HashMap<String, String> hostToLocalFile;
    private long length;
    private String[] hosts;
    private String[] dimensions;
    private String[] metrics;
    private String[] filters;

    public DruidSplit() {
    }

    public DruidSplit(HashMap hostMaps, long length, String[] hosts, String[] dimensions, String[] metrics, String[] filters) {
        this.hostToLocalFile = hostMaps;
        this.length = length;
        this.hosts = hosts;
        this.dimensions = dimensions;
        this.metrics = metrics;
        this.filters = filters;
    }

    public List<String> getLocalFiles() {
        return Lists.newArrayList(hostToLocalFile.values());
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return hosts;
    }

    public String[] getDimensions() {
        return dimensions;
    }

    public String[] getMetrics() {
        return metrics;
    }

    public String[] getFilters() {
        return filters;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        MapWritable mapObj = new MapWritable();
        for (Map.Entry obj : this.hostToLocalFile.entrySet()) {
            mapObj.put(new Text((String) obj.getKey()), new Text((String) obj.getValue()));
        }
        mapObj.write(dataOutput);
        dataOutput.writeLong(this.length);
        new ArrayWritable(dimensions).write(dataOutput);
        new ArrayWritable(metrics).write(dataOutput);
        new ArrayWritable(filters).write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hostToLocalFile = new HashMap<String, String>();
        MapWritable mapObj = new MapWritable();
        mapObj.readFields(dataInput);
        for (Map.Entry obj : mapObj.entrySet()) {
            this.hostToLocalFile.put(((Text) obj.getKey()).toString(), ((Text) obj.getValue()).toString());
        }
        this.length = dataInput.readLong();
        this.hosts = this.hostToLocalFile.keySet().toArray(new String[this.hostToLocalFile.keySet().size()]);
        ArrayWritable tmp = new ArrayWritable(UTF8.class);
        tmp.readFields(dataInput);
        this.dimensions = tmp.toStrings();
        tmp.readFields(dataInput);
        this.metrics = tmp.toStrings();
        tmp.readFields(dataInput);
        this.filters = tmp.toStrings();
    }
}
