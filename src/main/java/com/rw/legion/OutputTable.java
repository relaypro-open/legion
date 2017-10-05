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

import java.util.ArrayList;

/**
 * An <code>OutputTable</code> defines the structure of a table that Legion will
 * seek to write output to. It has a title, which will be the prefix of files
 * for the table in the output directory. It contains a list of indexes, which
 * can be used to normalize arrays from JSON input or values from  CSV data that
 * isn't appropriately normalized (e.g., id, value1, value2, value3). Finally,
 * it contains a list of <code>OutputColumn</code>s, which specify the order and
 * contents of the columns that will be written in this file as output.
 */

public class OutputTable {
    private String title;
    private ArrayList<String> indexes;
    private ArrayList<OutputColumn> columns;
    
    public OutputTable() {
        // Empty constructor requested by GSON.
    }
    
    /**
     * @return The table title, which is used as the output name for named
     * outputs in <code>MultipleOutputs</code>, as well as the prefix for the
     * files for this table in the output directory.
     */
    public String getTitle() {
        return title;
    }
    
    /**
     * @return An ArrayList of <code>OutputColumn</code>s, which specifies the
     * order and contents of columns that should appear in this table.
     */
    public ArrayList<OutputColumn> getColumns() {
        return columns;
    }
    
    /**
     * @return The keys used to look up the columns in the input data. This will
     * either be CSV column headers or JSON attributes. When normalizing arrays
     * or other de-normalized data using indexes, keys can contain index names
     * in angle brackets (e.g., sales&lt;monthIndex&gt;).
     */
    public String[] getColumnKeys() {
        String[] columnKeys = new String[columns.size()];
        
        for (int i = 0; i < columns.size(); i++) {
            columnKeys[i] = columns.get(i).getKey();
        }
        
        return columnKeys;
    }
    
    /**
     * @return Whether or not this output table is using Legion's index feature
     * to normalize arrays or other de-normalized data.
     */
    public boolean hasIndexes() {
        return indexes == null ? false : true;
    }
    
    /**
     * @return The names of the indexes being used by this table for normalizing
     * arrays or other repeated input data.
     */
    public ArrayList<String> getIndexNames() {
        return indexes;
    }
}
