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

/**
 * A column in a Legion <code>OutputTable</code>. The column contains a key
 * (CSV column header or JSON attribute), which is used to look up data in a
 * <code>LegionRecord</code>. It contains a data type, an optional regex, and a
 * substitute for nulls, to be used in data validation. Finally, it specifies
 * whether absent values should be treated as nulls or rejected, whether nulls
 * should be accepted, and whether something should be substituted for nulls.
 */

public class OutputColumn {
    private String key;
    private String dataType;
    private String regex;
    private Boolean allowNulls;
    private Boolean absentAsNull;
    private String nullSubstitute;
    
    private Pattern validationPattern;
    private ArrayList<String> indexes;
    private boolean hasIndexes;
    private Pattern keyPattern;
    private boolean initialized = false;
    private String validationReason;
    
    /**
     * Empty constructor for GSON.
     */
    public OutputColumn() {
        
    }
    
    private void initialize() {
        if (initialized == false) {
            // Set to default values
            allowNulls = allowNulls == null ? true : allowNulls;
            absentAsNull = absentAsNull == null ? true : absentAsNull;
            regex = regex == null ? "" : regex;
            nullSubstitute = nullSubstitute == null ? "" : nullSubstitute;
            
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
        initialize();
        
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
        initialize();
        
        return keyPattern;
    }
    
    /**
     * @return  A list of all indexes used in this column key.
     */
    public ArrayList<String> getIndexes() {
        initialize();
        
        return indexes;
    }
    
    /**
     * @return  Whether this column key uses indexes.
     */
    public boolean hasIndexes() {
        initialize();
        
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
        initialize();
        validationReason = null;
	
        // If the key is absent, either fail or set it to blank
        if (value.getData(keyOverride) == null) {
            if (absentAsNull) {
                value.setField(keyOverride, "");
            } else {
                validationReason = "key absent";
                return false;
            }
        }
        
        // If the value is blank, fail, replace it with something, or do nothing
        if (value.getData(keyOverride).equals("")) {
            if (allowNulls == false) {
                validationReason = "value blank";
                return false;
            } else if (!nullSubstitute.equals("")) {
                value.setField(keyOverride, nullSubstitute);
            }
        }
        
        /*
         * Unless this is a blank value and blanks are allowed, evaluate the
         * data to see if it matches the validation pattern for this column.
         */
        if ((value.getData(keyOverride).equals("") && allowNulls) == false) {
            if (validationPattern == null) {
                if (regex.equals("")) {
                    if (dataType.equals("String")) {
                        regex = ".*";
                    } else if (dataType.equals("Int")) {
                        regex = "^-?\\d+$";
                    } else if (dataType.equals("Float")) {
                        regex = "^-?\\d*\\.?\\d+$";
                    } else if (dataType.equals("Scientific")) {
                        regex = "^-?\\d*\\.?\\d+([eE][-+]?\\d+)?$";
                    } else if (dataType.equals("Boolean")) {
                        regex = "^0|1|True|False|true|false|T|F|t|f$";
                    }
                }
                
                validationPattern = Pattern.compile(regex);
            }
            
            if (validationPattern.matcher(value.getData(keyOverride))
                    .matches() == false) {
                validationReason = "regex";
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * @return  The reason the most recently validated value failed validation,
     *          or null if it passed validation.
     */
    public String getValidationReason() {
        return validationReason;
    }
}