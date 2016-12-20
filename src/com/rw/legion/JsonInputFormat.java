/*
 * Credit: This file includes code derived from the Apache Software Foundation's
 * Hadoop project (http://hadoop.apache.org/) and released under the Apache
 * License, Version 2.0.
 * 
 * Copyright (C) 2016 Republic Wireless
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rw.legion;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

import com.google.common.base.Charsets;

/** 
 * An <code>InputFormat</code> for <code>JsonRecordReader</code>. Essentially,
 * the default Hadoop <code>TextInputFormat</code> modified to use the
 * <code>JsonRecordReader</code>.
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class JsonInputFormat
        extends FileInputFormat<NullWritable, LegionRecord> {
    private LegionObjective legionObjective;

    @Override
    public RecordReader<NullWritable, LegionRecord>
            createRecordReader(InputSplit split, TaskAttemptContext context) {
        
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        
        byte[] recordDelimiterBytes = null;
        
        if (null != delimiter)
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        
        return new JsonRecordReader(recordDelimiterBytes);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
            CompressionCodec codec;
        Configuration job = context.getConfiguration();
        legionObjective =
                ObjectiveDeserializer.deserialize(job.get("legion_objective"));
        
        if (legionObjective.getCodecOverride() != null) {
            codec = new CompressionCodecFactory(context.getConfiguration())
                .getCodecByClassName(legionObjective.getCodecOverride());
        } else {
            codec = new CompressionCodecFactory(context.getConfiguration())
                .getCodec(file);
        }

        if (null == codec) {
            return true;
        }
        
        return codec instanceof SplittableCompressionCodec;
    }
}
