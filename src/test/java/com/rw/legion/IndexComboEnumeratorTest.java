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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexComboEnumeratorTest {
    private IndexComboEnumerator ice;

    @BeforeEach
    void setUp() {
        ice = new IndexComboEnumerator();
    }

    @Test
    void addCombo() {
        assertEquals(true, ice.getCombinations().isEmpty(), "Initialized empty");
        IndexCombo ic = new IndexCombo();
        ice.addCombo(ic);
        assertEquals(false, ice.getCombinations().isEmpty(), "Added combo");
    }

    @Test
    void getSize() {
        assertEquals(ice.getCombinations().size(), ice.getSize());
    }

    @Test
    void iterator() {
        assertEquals(false, ice.iterator().hasNext(), "Test hasNext() on empty iterator");
    }

}