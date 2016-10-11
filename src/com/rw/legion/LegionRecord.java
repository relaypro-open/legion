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
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;

import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;

import org.apache.hadoop.io.*;
import org.apache.commons.lang.StringUtils;

/**
 * At its core, this is nothing more than a hash map, which links data keys
 * (either CSV column headers or JSON attributes) to the values associated with
 * those keys (data in a CSV row, or JSON values).
 */
public class LegionRecord implements Writable{
    private HashMap<String, String> contents;

    public LegionRecord() {
        contents = new HashMap<String, String>();
    };

    /**
     * Enable Hadoop serialization.
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(contents.size());

        for (HashMap.Entry<String, String> entry : contents.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
    }
    
    /**
     * Enable Hadoop de-serialization.
     */
    public void readFields(DataInput in) throws IOException {
        contents.clear();
        
        int numVals = in.readInt();
        
        for (int i = 0; i < numVals; i++) {
            String key = in.readUTF();
            String value = in.readUTF();
            
            contents.put(key, value);
        }
    }
    
    /**
     * Set the contents of the <code>LegionRecord</code> to a row from a CSV
     * file.
     * 
     * @param fileName  The full path of the file this row came from. Can be
     *                  extracted later using the key "file_name".
     * @param header  CSV header, from which column keys will be extracted.
     * @param rawLine  CSV record, from which data will be extracted.
     */
    public void setFromCsv(String fileName, String[] header, String rawLine) {
        contents.put("file_name", fileName);
        
        /*
         *  Supply the header length to split() in case there are trailing null
         *  fields.
         */
        String[] lineParts = rawLine.split(",", header.length);
        
        if (header.length == lineParts.length) {
            for (int i = 0; i < lineParts.length; i++) {
                contents.put(header[i],  lineParts[i]);
            }
        }
    }
    
    /**
     * Set the contents of the <code>LegionRecord</code> using a JSON object.
     * 
     * @param fileName  The full path of the file this row came from. Can be
     *                  extracted later using the key "file_name".
     * @param rawLine  A JSON object, from which keys and data will be
     *                 extracted.
     */
    public void setFromJson(String fileName, String rawLine) {
        contents.put("file_name", fileName);
        
        try {
            JsonReader jsonReader = new JsonReader(new StringReader(rawLine));
            jsonReader.setLenient(true);
            JsonParser jsonParser = new JsonParser();
            JsonElement mainElement = jsonParser.parse(jsonReader);
            traverseJson("$", mainElement);
        } catch(JsonSyntaxException e) {
            /*
             * Should probably make it explicit in code that this gets handled
             * gracefully in the future. For now, all keys in the record will be
             * absent, so the record will simply fail to pass validation and be
             * ignored (unless absentAsNull is true for all columns, in which
             * case the row will be all nulls).
             */
        }
    }
    
    /**
     * Set an individual field in this record.
     * 
     * @param key  The key to be set.
     * @param value  The data to set for the supplied key.
     */
    public void setField(String key, String value) {
        contents.remove(key);
        contents.put(key, value);
    }
    
    /**
     * Look up the data value associated with a particular key.
     * 
     * @param key  The key to look up.
     * @return  The data value associated with the supplied key.
     */
    public String getData(String key) {
        if (contents.containsKey(key)) {
            return contents.get(key);
        } else {
            return null;
        }
    }
    
    /**
     * Look up the data values associated with an array of keys.
     * 
     * @param keys  The keys to look up.
     * @return  The data values associated with the supplied keys.
     */
    public String[] getData(String[] keys) {
        String[] values = new String[keys.length];
        
        for (int i = 0; i < keys.length; i++) {
            values[i] = contents.get(keys[i]);
        }
        
        return values;
    }
    
    /**
     * Using information from a <code>legionObjective</code>, find all possible
     * combinations of index values that appear in this record for the
     * particular set of indexes used by each <code>OutputTable</code> in the
     * objective.
     * 
     * @param legionObjective  The objective to use for finding index values.
     * @return  A map linking a comma-separated list of index names to a set of
     *          all combinations of index values for those indexes that appears
     *          in this record. The list of index names used in each key is
     *          determined by the <code>ArrayList</code> of the indexes in a
     *          <code>LegionOutput</code>, so order is guaranteed. Each entry in
     *          each set is a map linking index keys to index values.
     */
    public HashMap<String, HashSet<HashMap<String, String>>>
            findIndexValues(LegionObjective legionObjective) {
        
        HashMap<String, HashSet<HashMap<String, String>>> indexValues =
            new HashMap<String, HashSet<HashMap<String, String>>>();
        
        for (OutputTable outputTable : legionObjective.getOutputTables()) {
            if (outputTable.hasIndexes()) {
                ArrayList<String> indexes = outputTable.getIndexNames();
                String indexKey = StringUtils.join(indexes, ",");
                
                for (OutputColumn outputColumn : outputTable.getColumns()) {
                    if (outputColumn.hasIndexes()) {
                        /*
                         * Get a pattern that will match data keys that are
                         * equivalent to the output column key with index values
                         * substituted for index names, then check the keys in
                         * this record for matches, and extract the index
                         * values.
                         */
                        Pattern pattern = outputColumn.getKeyPattern();
                        
                        for (String dataKey : contents.keySet()) {
                            Matcher matcher = pattern.matcher(dataKey);
                            
                            if (matcher.matches() && matcher.groupCount()
                                    == outputTable.getIndexNames().size()) {
                                
                                HashMap<String, String> values =
                                    new HashMap<String, String>();
                                
                                /*
                                 * Since order of indexes in the data key could
                                 * be different than specified for the table as
                                 * a whole (e.g., objective lists idxA, idxB but
                                 * key is user<idxB>val<idxA>).
                                 */
                                ArrayList<String> indexOrder =
                                    outputColumn.getIndexes();
                                
                                for (int i = 0; i < matcher.groupCount(); i++) {
                                    values.put(indexOrder.get(i),
                                        matcher.group(i + 1));
                                }
                                
                                if (indexValues.containsKey(indexKey) ==
                                        false) {
                                    indexValues.put(indexKey,
                                        new HashSet<HashMap<String, String>>());
                                }
                                
                                indexValues.get(indexKey).add(values);
                            }
                        }
                    }
                }
            }
        }
        
        return indexValues;
    }
    
    /**
     * Recursively traverses all levels of a JSON object and adds their contents
     * to the <code>LegionRecord</code>. Keys are formatted using a simplified
     * JSON path with dot notation (http://goessner.net/articles/JsonPath/).
     * 
     * @param location  The JSON path leading up to the current depth level.
     * @param element  An element that appears at the current depth level.
     */
    private void traverseJson(String location, JsonElement element) {
        if (element.isJsonNull()) {
            contents.put(location, "");
        } else if (element.isJsonPrimitive()) {
            contents.put(location, element.getAsString());
        } else if (element.isJsonObject()) {
            for (Map.Entry<String, JsonElement> entry :
                    element.getAsJsonObject().entrySet()) {
                traverseJson(location + "." + entry.getKey(), entry.getValue());
            }
        } else if (element.isJsonArray()) {
            for (int i = 0; i < element.getAsJsonArray().size(); i++ ) {
                traverseJson(location + "[" + new Integer(i).toString() + "]",
                    element.getAsJsonArray().get(i));
            }
        }
    }
}