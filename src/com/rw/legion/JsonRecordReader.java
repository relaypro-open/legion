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

import java.util.Map;

import com.google.gson.JsonSyntaxException;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;
import com.google.gson.stream.JsonReader;

import java.io.StringReader;

/**
 * Produces <code>NullWritable</code> keys and <code>LegionRecord</code> values.
 * There is one <code>LegionRecord</code> per line in a file, each of which
 * must contain a valid JSON object. Within the LegionRecord, JSON nodes are
 * represented using a simplified JSON path with dot notation (see 
 * http://goessner.net/articles/JsonPath/). Values are data stored at that path.
 */
public class JsonRecordReader extends LegionRecordReader {
    private LegionRecord record;
    
    public JsonRecordReader(byte[] recordDelimiterBytes) {
        super(recordDelimiterBytes);
    }
    
    /**
     * Takes a line in JSON format, parses it, and builds a
     * <code>LegionRecord</code> with JSON paths mapped to the data found at
     * that path.
     * 
     * @return A legion record containing JSON paths mapped to the data found
     *         at that path.
     */
    protected LegionRecord makeRecord() {
        String lineString = currentLine.toString();
        record = new LegionRecord();
        record.setField("file_name", fileName);
        record.setField("file_line", Long.toString(currentLineNumber));
        
        try {
            JsonReader jsonReader =
                    new JsonReader(new StringReader(lineString));
            jsonReader.setLenient(true);
            JsonParser jsonParser = new JsonParser();
            JsonElement mainElement = jsonParser.parse(jsonReader);
            traverseJson("$", mainElement);
        } catch(JsonSyntaxException e) {
           return null;
        }
        
        return record;
    }
    
    /**
     * Recursively traverses all levels of a JSON object and adds their contents
     * to the <code>LegionRecord</code>.
     * 
     * @param location  The JSON path leading up to the current depth level.
     * @param element  An element that appears at the current depth level.
     */
    private void traverseJson(String location, JsonElement element) {
        if (element.isJsonNull()) {
            record.setField(location, "");
        } else if (element.isJsonPrimitive()) {
            record.setField(location, element.getAsString());
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
