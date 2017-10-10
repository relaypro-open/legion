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
import com.google.gson.JsonParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FloatCheckerTest {
    private FloatChecker fcUnspecified;
    private FloatChecker fcDoubleUpper;
    private FloatChecker fcDouble;
    private FloatChecker fcFloat;

    private String floatTypeDoubleUpper = "DoUbLe";
    private String floatTypeDouble = "double";
    private String floatTypeFloat = "float";
    private String floatTypeInvalid = "foobar";

    private static String maxDouble = Double.toString(Double.MAX_VALUE);  // 1.7976931348623157E308
    private static String minDouble = Double.toString(Double.MIN_VALUE);  // 4.9E-324
    private static String tooBigDouble = "1.7976931348623157E309";
    private static String nearZeroDouble = "4.9E-325";

    private static String maxFloat = Float.toString(Float.MAX_VALUE);  // 3.4028235E38
    private static String minFloat = Float.toString(Float.MIN_VALUE);  // 1.4E-45
    private static String tooBigFloat = "3.4028235E39";
    private static String nearZeroFloat = "1.4E-46";

    private static String notNumber = "FooBar";

    private JsonObject buildJson (String floatType) {
        if (floatType == null) {
            return new JsonObject();
        }

        String json = "\n" +
                "{\n" +
                "  \"floatType\": \"" + floatType + "\"\n" +
                "}";

        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse(json).getAsJsonObject();

        return obj;
    }
    @BeforeEach
    void setUp() throws FloatChecker.InvalidFloatTypeException {
        fcUnspecified = new FloatChecker(buildJson(null));
        fcDoubleUpper = new FloatChecker(buildJson(floatTypeDoubleUpper));
        fcDouble = new FloatChecker(buildJson(floatTypeDouble));
        fcFloat = new FloatChecker(buildJson(floatTypeFloat));
    }

    @Test
    void throwsInvalidFloatTypeException() {
        assertThrows(FloatChecker.InvalidFloatTypeException.class, () -> new FloatChecker(buildJson(floatTypeInvalid)));
    }

    @Test
    void validatesDoubleUpperVar() {
        assertEquals(floatTypeDouble, fcDoubleUpper.getFloatType());
    }

    @Test
    void validatesDouble() {
        assertEquals(floatTypeDouble, fcUnspecified.getFloatType());
        assertEquals(true, fcDouble.validates(maxDouble), "Max Double Value");
        assertEquals(true, fcDouble.validates(minDouble), "Min Double Value");
        assertEquals(false, fcDouble.validates(tooBigDouble), "Exceeds Max Double Value");
        assertEquals(true, fcDouble.validates(nearZeroDouble), "Near Zero Double Value");
        assertEquals(false, fcDouble.validates(notNumber), "Not a Number");
    }

    @Test
    void validatesFloat() {
        assertEquals(floatTypeFloat, fcFloat.getFloatType());
        assertEquals(true, fcFloat.validates(maxFloat), "Max Float Value");
        assertEquals(true, fcFloat.validates(minFloat), "Min Float Value");
        assertEquals(false, fcFloat.validates(tooBigFloat), "Exceeds Max Float Value");
        assertEquals(true, fcFloat.validates(nearZeroFloat), "Near Zero Float Value");
        assertEquals(false, fcFloat.validates(notNumber), "Not a Number");
    }

    @Test
    void validatesUnspecifiedType() {
        assertEquals(floatTypeDouble, fcUnspecified.getFloatType());
    }

}
