/*
 * Credit: This file includes code derived from the Apache Software Foundation's
 * Hadoop project (http://hadoop.apache.org/) and released under the Apache
 * License, Version 2.0.
 * 
 * Copyright (C) 2017 Republic Wireless
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

package com.rw.legion.input;

import com.rw.legion.LegionRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;

import java.io.IOException;

/**
 * Input format that is a <code>CombineFileInputFormat</code>-equivalent for
 * <code>CsvInputFormat</code>.
 * 
 * @see CsvInputFormat
 */

public class CombineCsvInputFormat
        extends CombineFileInputFormat<NullWritable, LegionRecord> {
    
    public RecordReader<NullWritable, LegionRecord> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        
        return new CombineFileRecordReader<NullWritable, LegionRecord>(
                (CombineFileSplit)split, context,
                LegionRecordReaderWrapper.class);
    }
    
    @Override
    protected boolean isSplitable(JobContext job, Path file) {
        // We should never split this, because we're already combining files.
        return false;
    }
    
    /**
     * Record reader that may be passed to <code>CombineFileRecordReader</code>
     * so it can be used in a <code>CombineFileInputFormat</code>-equivalent
     * for <code>LegionInputFormat</code>.
     *
     * @see LegionInputFormat
     */
    private static class LegionRecordReaderWrapper
            extends CombineFileRecordReaderWrapper<NullWritable, LegionRecord> {
        
        // This constructor signature is required by CombineFileRecordReader.
        public LegionRecordReaderWrapper(CombineFileSplit split,
                TaskAttemptContext context, Integer idx)
                throws IOException, InterruptedException {
            
            super(new CsvInputFormat(), split, context, idx);
        }
    }
}