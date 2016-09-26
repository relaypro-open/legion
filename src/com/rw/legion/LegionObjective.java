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

import java.util.ArrayList;
import com.google.gson.Gson;

/**
 * A <code>LegionObjective</code> is a definition of a task that Legion should
 * accomplish with the input data. It specifies the type of the input data (CSV
 * vs. JSON), the codec to be used for reading that data, and lists of
 * <code>OutputTable</code>s and <code>OutputColumns</code> to be extracted
 * from the input data.
 * 
 * Contents of <code>LegionObjective</code>s are de-serialized from JSON files
 * using GSON.
 */

public class LegionObjective {
    private ParsedJson parsedJson;
    
    /**
     * @param json  The raw JSON to de-serialize from.
     */
    public LegionObjective(String json) {
        Gson gson = new Gson();
        parsedJson = gson.fromJson(json, ParsedJson.class);
    }
    
    /**
     * Container that objective JSON will actually de-serialize to.
     */
    private class ParsedJson {
        public String inputDataType;
        public String codecOverride;
        public boolean combineFiles;
        public int maxCombinedSize;
        public ArrayList<OutputTable> outputTables;
        
        @SuppressWarnings("unused")
        public ParsedJson() {
            // Empty constructor requested by GSON.
        }
    }
    
    /**
     * @return  Either JSON or CSV, depending on data type.
     */
    public String getInputDataType() {
        return parsedJson.inputDataType;
    }
    
    /**
     * @return  Either null (no codec override) or the canonical class name of
     * the Hadoop codec to use (e.g., org.apache.hadoop.io.compress.GzipCodec).
     */
    public String getCodecOverride() {
        return parsedJson.codecOverride;
    }
    
    /**
     * @return  An <code>ArrayList</code> of output tables for this objective.
     */
    public ArrayList<OutputTable> getOutputTables() {
        return parsedJson.outputTables;
    }
    
    /**
     * @return  Whether or not input files should be combined for this job.
     */
    public boolean getCombineFiles() {
        return parsedJson.combineFiles;
    }
    
    /**
     * @return  Maximum size of an input split to create by combining input
     * files with <code>CombineLegionInputFormat</code>.
     */
    public int getMaxCombinedSize() {
        return parsedJson.maxCombinedSize;
    }
}