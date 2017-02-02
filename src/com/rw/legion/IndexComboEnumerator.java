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

import java.util.HashSet;
import java.util.Iterator;

/**
 * A glorified <code>HashSet</code> which keeps track of all combinations
 * of indexes appearing for a particular <code>OutputTable</code> in a
 * particular <code>LegionRecord</code>.
 */
public class IndexComboEnumerator implements Iterable<IndexCombo> {
    private HashSet<IndexCombo> combinations;
    
    public IndexComboEnumerator() {
        combinations = new HashSet<IndexCombo>();
    }
    

    /**
     * Add an index combination to the list.
     * 
     * @param index  A combination of index keys/values to be added to the list.
     */
    public void addCombo(IndexCombo indexCombo) {
        combinations.add(indexCombo);
    }
    
    /**
     * Get number of combinations currently in the list.
     * 
     * @return  Number of combinations currently in the list.
     */
    public int getSize() {
        return combinations.size();
    }
    
    /**
     * Get an iterator for looping over all index combinations in the list.
     * 
     * @return  An iterator to loop through the combinations in this list.
     */
    public Iterator<IndexCombo> iterator() {
        return combinations.iterator();
    }
}