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

package com.rw.legion.columntransform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class NullReplaceTransformerTest {

    private NullReplaceTransformer npt;
    private String replacement = "bar";

    private JsonObject buildJson (String replacement) {
        String json = "\n" +
                "{\n" +
                "  \"replacement\": \"" + replacement + "\"\n" +
                "}";

        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse(json).getAsJsonObject();

        return obj;
    }

    @BeforeEach
    void setUp() {
        npt = new NullReplaceTransformer(buildJson(replacement));
    }

    @Test
    void transformStr() {
        assertEquals(replacement, npt.transform(""));
    }

    @Test
    void transformNull() {
        assertEquals(replacement, npt.transform(null));
    }

    @Test
    void transformNoReplace() {
        String mystring = "mystring";
        assertEquals(mystring, npt.transform(mystring));
    }

    @Test
    void fail() {
        assertEquals(false, true);
    }

}
