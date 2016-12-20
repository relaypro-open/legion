/*
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
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;

import org.apache.hadoop.io.compress.*;

/**
* Default Legion job which can be used to run Legion without any custom code.
* Reads a Legion JSON objective file, identifies target output tables and
* columns, cleans incoming data, structures it appropriately, and writes the
* results in gzip format.
*/

public class DefaultJob {
    
    /**
     * Main method.
     * 
     * @param args  Arguments should be: 1) input path, 2) output path, 3)
     * location of Legion objective file.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Load the Legion objective from the JSON doc.
        Path path = new Path(args[2]);
        FileSystem fs = FileSystem.get(new URI(args[2]), conf);
        BufferedReader br
            = new BufferedReader(new InputStreamReader(fs.open(path)));
        String json = "";
        
        String line = br.readLine();
        
        while (line != null){
            json += line;
            line = br.readLine();
        }
        
        br.close();
        
        /*
         *  Save the JSON for the Legion objective to the Hadoop configuration,
         *  so we can access it in other containers.
         */
        conf.setStrings("legion_objective", json);
        
        // De-serialize the objective so we can access the settings here.
        LegionObjective legionObjective =
                ObjectiveDeserializer.deserialize(json);
        
        // Start configuring the MapReduce job.
        Job hadoopJob = Job.getInstance(conf, "Legion");
        
        hadoopJob.setJarByClass(DefaultJob.class);
        hadoopJob.setMapperClass(DefaultMapper.class);
        LazyOutputFormat.setOutputFormatClass(hadoopJob,
                TextOutputFormat.class);
        
        // Compress the output to speed things up.
        TextOutputFormat.setCompressOutput(hadoopJob, true);
        TextOutputFormat.setOutputCompressorClass(hadoopJob, GzipCodec.class);
        
        // What input format do we use?
        Class<? extends FileInputFormat<NullWritable, LegionRecord>> inputClass;
        
        if (legionObjective.getCombineFiles()) {
            if (legionObjective.getInputDataType().equals("CSV")) {
                inputClass = CombineCsvInputFormat.class;
            } else {
                inputClass = CombineJsonInputFormat.class;
            }
            
            CombineFileInputFormat.setMaxInputSplitSize(hadoopJob,
                    legionObjective.getMaxCombinedSize());
        } else {
            if (legionObjective.getInputDataType().equals("CSV")) {
                inputClass = CsvInputFormat.class;
            } else {
                inputClass = JsonInputFormat.class;
            }
        }
       
        hadoopJob.setInputFormatClass(inputClass);
    
        /* 
         * These are just static convenience methods, so it doesn't matter if
         * they come from the wrong class.
         */
        FileInputFormat.setInputDirRecursive(hadoopJob, true);
        FileInputFormat.addInputPath(hadoopJob, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(hadoopJob, new Path(args[1]));
        
        // Since a Legion objective can specify multiple output tables.
        for (OutputTable outputTable : legionObjective.getOutputTables()) {
            MultipleOutputs.addNamedOutput(hadoopJob, outputTable.getTitle(),
                    TextOutputFormat.class, NullWritable.class, Text.class);
        }
        
        MultipleOutputs.addNamedOutput(hadoopJob, "skipped",
                TextOutputFormat.class, NullWritable.class, Text.class);
        
        hadoopJob.waitForCompletion(true);
    } 
}