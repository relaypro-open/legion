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

package main.java.com.rw.legion;

import java.util.ArrayList;

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
    public String inputFormat;
    public String codecOverride;
    public Integer maxCombinedSize;
    public ArrayList<OutputTable> outputTables;
    
    /**
     * Constructor required by Gson.
     */
    public LegionObjective() {
        
    }
    
    /**
     * @return  Returns the canonical class name of the input format to use for
     * this job.
     */
    public String getInputFormat() {
        return inputFormat;
    }
    
    /**
     * @return  Either null (no codec override) or the canonical class name of
     * the Hadoop codec to use (e.g., org.apache.hadoop.io.compress.GzipCodec).
     */
    public String getCodecOverride() {
        return codecOverride;
    }
    
    /**
     * @return  An <code>ArrayList</code> of output tables for this objective.
     */
    public ArrayList<OutputTable> getOutputTables() {
        return outputTables;
    }
    
    /**
     * @return  Maximum size of an input split to create by combining input
     * files with <code>CombineLegionInputFormat</code>.
     */
    public Integer getMaxCombinedSize() {
        return maxCombinedSize;
    }
}