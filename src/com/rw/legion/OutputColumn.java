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

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.HashMap;
import com.rw.legion.columncheck.ColumnChecker;
import com.rw.legion.columntransform.ColumnTransformer;

/**
 * A column in a Legion <code>OutputTable</code>. The column contains a key
 * (CSV column header or JSON attribute), which is used to look up data in a
 * <code>LegionRecord</code>. It contains a data type, an optional regex, and a
 * substitute for nulls, to be used in data validation. Finally, it specifies
 * whether absent values should be treated as nulls or rejected, whether nulls
 * should be accepted, and whether something should be substituted for nulls.
 */

public class OutputColumn {
    // These will be automatically de-serialized by Gson.
    private String key;
    private Boolean failOnAbsent = false;
    private Boolean failOnNull = false;
    private Boolean failOnValidation = false;
    
    // These will get set up when initialize() is called.
    private ColumnChecker checker;
    private ColumnTransformer transformer;
    private ArrayList<String> indexes;
    private boolean hasIndexes;
    private Pattern keyPattern;
    private String failureReason;
    
    /**
     * Empty constructor for GSON.
     */
    public OutputColumn() {
        
    }
    
    public void initialize(ColumnChecker checker,
            ColumnTransformer transformer) {
        this.checker = checker;
        this.transformer = transformer;
        indexes = new ArrayList<String>();
        hasIndexes = false;
        String keyRegex = "^\\Q" + key + "\\E$";
        Matcher m = Pattern.compile("<.*?>").matcher(key);
        
        while (m.find()) {
            hasIndexes = true;
            indexes.add(m.group());
            keyRegex = keyRegex.replace(m.group(), "\\E([0-9]+)\\Q");
        }
        
        keyPattern = Pattern.compile(keyRegex);
    }
    
    /**
     * @return  The CSV column header or JSON attribute to extract from the
     * <code>LegionRecord</code> and place in this column.
     */
    public String getKey() {
        return key;
    }
    
    /**
     * Get the key for this column, replacing index names in the key with the
     * specified values of the indexes.
     * 
     * @param indexValues  A map of index names and values to substitute in this
     *                     key.
     * @return  The CSV column header or JSON attribute to extract from the
     *          <code>LegionRecord</code> and place in this column.
     */
    public String getKey(HashMap<String, String> indexValues) {
        String tempKey = key;
        
        for (HashMap.Entry<String, String> entry : indexValues.entrySet()) {
            tempKey = tempKey.replace(entry.getKey(), entry.getValue());
        }
        
        return tempKey;
    }
    
    /**
     * @return  A pattern that will match any CSV column header or JSON
     *          attribute that is equivalent to this key, or this key with index
     *          values substituted for index names. Used for finding all
     *          combinations of index values present in a file.
     */
    public Pattern getKeyPattern() {
        return keyPattern;
    }
    
    /**
     * @return  A list of all indexes used in this column key.
     */
    public ArrayList<String> getIndexes() {
        return indexes;
    }
    
    /**
     * @return  Whether this column key uses indexes.
     */
    public boolean hasIndexes() {
        return hasIndexes;
    }
    
    /**
     * @param value  The <code>LegionRecord</code> in which to look for this
     *               column's key.
     * @return  Whether or not valid data for this column's key can be found in
     *          the supplied <code>LegionRecord</code>.
     */
    public boolean validates(LegionRecord value) {
        return validates(key, value);
    }
    
    /**
     * @param keyOverride  Override the key to look up in the
     *                     <code>LegionRecord</code> when evaluating this
     *                     column. Used when the column has indexes.
     * @param value  The <code>LegionRecord</code> in which to look for this
     *               column's key.
     * @return  Whether or not valid data for this column's key can be found in
     *          the supplied <code>LegionRecord</code>.
     */
    public boolean validates(String keyOverride, LegionRecord value) {
        failureReason = null;
	
        /*
         *  If the key is absent, either fail the record or set it to null
         *  (blank).
         */
        if (value.getData(keyOverride) == null) {
            if (failOnAbsent) {
                failureReason = "key absent";
                return false;
            } else {
                value.setField(keyOverride, "");
            }
        }
        
        // If the value is null (blank), either fail the record or do nothing.
        if (value.getData(keyOverride).equals("")) {
            if (failOnNull) {
                failureReason = "null not allowed";
                return false;
            }
        }
        
        /*
         * Unless this is a null (blank) value, validate the data using the
         * <code>ColumnCheck</code> for this column.
         */
        if (! value.getData(keyOverride).equals("")) {
            if (! checker.validates(value.getData(keyOverride))) {
                /*
                 * Fail the record if necessary (including if failOnNull is
                 * true, because then we can't replace with null.
                 */
                if (failOnValidation || failOnNull) {
                    failureReason = "data validation failed";
                    return false;
                } else {
                    value.setField(keyOverride,  "");
                }
            }
        }
        
        return true;
    }
    
    public void transform(LegionRecord value) {
        if (transformer != null) {
            value.setField(key, transformer.transform(value.getData(key)));
        }
    }
    
    /**
     * @return  The reason the most recently validated value failed validation,
     *          or null if it passed validation.
     */
    public String getFailureReason() {
        return failureReason;
    }
}