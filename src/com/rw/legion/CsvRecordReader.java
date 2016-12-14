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

/**
 * Produces <code>NullWritable</code> keys and <code>LegionRecord</code> values.
 * There is one <code>LegionRecord</code> per line in a file, each of which
 * must contain a valid CSV row. Within the <code>LegionRecord</code>, CSV
 * column headers are mapped to the data found for that column on a specific
 * row.
 */
public class CsvRecordReader extends LegionRecordReader {
    private String[] header;
    
    public CsvRecordReader(byte[] recordDelimiterBytes) {
        super(recordDelimiterBytes);
    }
    
    /**
     * Takes a line in CSV format, parses it, and builds a
     * <code>LegionRecord</code> with CSV column headers mapped to the data
     * found for that column on this row.
     * 
     * @return A legion record containing CSV column headers mapped to data
     * found in that column on this row.
     */
    protected LegionRecord makeRecord() {
        String lineString = currentLine.toString();
        
        if (currentLineNumber == 1) {
            header = lineString.split(",");
            return null;
        }
        
        LegionRecord record = new LegionRecord();
        record.setField("file_name", fileName);
        record.setField("file_line", Long.toString(currentLineNumber));
        
        /*
         *  Supply the header length to split() in case there are trailing null
         *  fields.
         */
        String[] lineParts = lineString.split(",", header.length);
        
        if (header.length == lineParts.length) {
            for (int i = 0; i < lineParts.length; i++) {
                record.setField(header[i],  lineParts[i]);
            }
        }
        
        return record;
    }
}
