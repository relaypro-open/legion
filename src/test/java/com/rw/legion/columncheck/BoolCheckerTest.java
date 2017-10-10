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

package com.rw.legion.columncheck;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BoolCheckerTest {
    private BoolChecker bc;
    private JsonObject obj = new JsonObject();

    @BeforeEach
    void setUp() {
        bc = new BoolChecker(obj);
    }

    @Test
    void validatesWordUpper() {
        assertEquals(true, bc.validates("FALSE"));
        assertEquals(true, bc.validates("TRUE"));
    }

    @Test
    void validatesWordLower() {
        assertEquals(true, bc.validates("false"));
        assertEquals(true, bc.validates("true"));
    }

    @Test
    void validatesLetter() {
        assertEquals(true, bc.validates("F"));
        assertEquals(true, bc.validates("T"));
        assertEquals(true, bc.validates("f"));
        assertEquals(true, bc.validates("t"));
    }

    @Test
    void validatesInt() {
        assertEquals(true, bc.validates("0"));
        assertEquals(true, bc.validates("1"));
        assertEquals(false, bc.validates("3"));
        assertEquals(false, bc.validates("4"));
    }

}
