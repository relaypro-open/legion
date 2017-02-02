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

import java.util.HashMap;

/**
 * A glorified <code>HashMap</code> mapping index names to index values for a
 * particular combination of indexes appearing in a <code>LegionRecord</code>.
 */
public class IndexCombo {
    private HashMap<String, String> hashMap;
    
    public IndexCombo() {
        hashMap = new HashMap<String, String>();
    }
    
    /**
     * Add an index name and value to this particular combination.
     * 
     * @param index  Name of the index.
     * @param value  Value of the index appearing in the data.
     */
    public void addIndex(String index, String value) {
        hashMap.put(index, value);
    }
    
    /**
     * Retrieve the value a particular index took in this combination.
     * 
     * @param index  Name of the index to retrieve a value for.
     * return  The value the named index took in this combination.
     */
    public String getValue(String index) {
        return(hashMap.get(index));
    }
    
    /**
     * Get a copy of the hashMap to facilitate overriding equals.
     * 
     * @return  The hashMap.
     */
    public HashMap<String, String> getHashMap() {
        return hashMap;
    }
    
    /**
     * Override default <code>hashCode()</code> so that the
     * <code>IndexComboEnumerator</code> will know when IndexCombos are
     * equivalent.
     */
    @Override
    public int hashCode() {
        return hashMap.hashCode();
    }
    
    /**
     * Override default <code>equals()</code> so that the
     * <code>IndexComboEnumerator</code> will know when IndexCombos are
     * equivalent.
     */
    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof IndexCombo)) return false;
        if (obj == this) return true;
        
        return hashMap.equals(((IndexCombo) obj).getHashMap());
    }
}
