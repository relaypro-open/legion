/*
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

package com.rw.legion;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Default Mapper class used by Legion. Takes a <code>NullWritable</code> key
 * and a <code>LegionRecord</code> value, loops through all output tables and
 * columns specified by the current <code>LegionObjective</code>, validates and
 * cleans the data, and yields a <code>NullWritable</code> key and a
 * CSV-formatted <code>Text</code> value, which will be written to a file by
 * the TextOutputFormat.
 */

public class DefaultMapper
    extends Mapper<NullWritable, LegionRecord, NullWritable, Text> {
    
    protected LegionObjective objective;
    protected MultipleOutputs<NullWritable, Text> outputWriters;
    private Text outputLine = new Text();
    private NullWritable nothing = NullWritable.get();
    
    /**
     * Do standard Hadoop setup, de-serialize the <code>LegionObjective</code>,
     * and prepare for writing to multiple output files.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void setup(Context context) {
        Configuration config = context.getConfiguration();
        this.objective = ObjectiveDeserializer.deserialize(
                config.get("legion_objective"));
        outputWriters = new MultipleOutputs(context);
    }
    
    /**
     * Default Mapper used by Legion. Takes a <code>NullWritable</code> key and
     * a <code>LegionRecord</code> value, loops through all output tables and
     * columns specified by the current <code>LegionObjective</code>, validates
     * and cleans the data, and yields a <code>NullWritable</code> key and a
     * CSV-formatted <code>Text</code> value, which will be written to a file by
     * the TextOutputFormat.
     * 
     * @param key  A <code>NullWritable</code>.
     * @param value  The current LegionRecord being worked on.
     * @param context  The Hadoop <code>Context</code>.
     */
    public void map(NullWritable key, LegionRecord value, Context context)
            throws IOException, InterruptedException {
        
        for (OutputTable outputTable : objective.getOutputTables()) {
            if (outputTable.hasIndexes()) {
                IndexComboEnumerator enumerator =
                        value.findIndexValues(outputTable);
                
                // No need to output this table if there were no index values
                if (enumerator.getSize() > 0) {
                    for (IndexCombo indexCombo : enumerator) {
                        /*
                         * Generate a list of keys to extract from the
                         * LegionRecord by replacing index names with current
                         * index values. Then try to extract data for those keys
                         * and output.
                         */
                        String[] modifiedKeys
                            = outputTable.getColumnKeys().clone();
                        
                        for (int i = 0; i < modifiedKeys.length; i++) {
                            for (String name : outputTable.getIndexNames()) {
                                name = "<" + name + ">";
                                
                                modifiedKeys[i] = modifiedKeys[i].replace(name,
                                        indexCombo.getValue(name));
                            }
                        }
                        
                        tryOutput(outputTable, value, modifiedKeys);
                    }
                }
            } else {
                
                /*
                 *  We don't have any indexes, so just try to extract data and
                 *  output!
                 */
                tryOutput(outputTable, value);
            }
        }
    }
    
    /**
     * Validates the data flowing to each output column, and writes output.
     * 
     * @param output  The current <code>OutputTable</code>.
     * @param value  The current <code>LegionRecord</code>.
     */
    private void tryOutput(OutputTable output, LegionRecord value)
            throws IOException, InterruptedException {
        
        // We're not overriding column keys, so just use the standard ones.
        tryOutput(output, value, output.getColumnKeys());
    }
    
    /**
     * Validates the data flowing to each output column, and writes output.
     * 
     * @param output  The current <code>OutputTable</code>.
     * @param value  The current <code>LegionRecord</code>.
     * @param keyList  A list of data keys to extract from the <code>
     *                 LegionRecord</code>.
     */
    private void tryOutput(OutputTable outputTable, LegionRecord value,
                           String[] keyList) throws IOException, InterruptedException {
        int i = 0;
        boolean validates = true;
        
        String[] dataToWrite = new String[outputTable.getColumns().size()];
        
        for (OutputColumn column : outputTable.getColumns()) {
            if (column.validates(keyList[i], value)) {
                column.transform(keyList[i], value);
                
                dataToWrite[i]
                    = StringEscapeUtils.escapeCsv(value.getData(keyList[i]));
            } else {
                dataToWrite = new String[4];
                dataToWrite[0] = value.getData("file_name");
                dataToWrite[1] = value.getData("file_line");
                dataToWrite[2] = column.getKey();
                dataToWrite[3] = column.getFailureReason();
                        
                validates = false;
                break;
            }
            
            i++;
        }
        
        outputLine.set(StringUtils.join(dataToWrite, ","));
        
        if (validates) {
            outputWriters.write(outputTable.getTitle(), nothing, outputLine,
                    outputTable.getTitle());
        } else {
            outputWriters.write("skipped", nothing, outputLine, "skipped");
        }
    }
    
    /**
     * Standard Hadoop cleanup.
     */
    public void cleanup(Context context)
            throws IOException, InterruptedException {
        outputWriters.close();
    }
}
