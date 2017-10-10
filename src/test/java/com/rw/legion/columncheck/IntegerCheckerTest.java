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

class IntegerCheckerTest {
    private IntegerChecker icUnspecified;
    private IntegerChecker icShortUpper;
    private IntegerChecker icShort;
    private IntegerChecker icInt;
    private IntegerChecker icLong;

    private String intTypeShortUpper = "ShOrT";
    private String intTypeShort = "short";
    private String intTypeInt = "int";
    private String intTypeLong = "long";
    private String intTypeInvalid = "fooBar";

    private int safeLengthShort = 4;
    private int safeLengthInt = 9;
    private int safeLengthLong = 18;

    private String validShort = "1234";
    private String validShortNeg = "-1234";
    private String validInt = "123456789";
    private String validIntNeg = "-123456789";
    private String validLong = "123456789012345";
    private String validLongNeg = "-123456789012345";

    private String invalidFloat = "1f";
    private String invalidDouble = "1.0";
    private String invalidString = "FooBar";

    private JsonObject buildJson (String intType) {
        if (intType == null) {
            return new JsonObject();
        }

        String json = "\n" +
                "{\n" +
                "  \"intType\": \"" + intType + "\"\n" +
                "}";

        JsonParser parser = new JsonParser();
        JsonObject obj = parser.parse(json).getAsJsonObject();

        return obj;
    }

    @BeforeEach
    void setUp() throws IntegerChecker.InvalidIntTypeException {
        icUnspecified = new IntegerChecker(buildJson(null));
        icShortUpper = new IntegerChecker(buildJson(intTypeShortUpper));
        icShort = new IntegerChecker(buildJson(intTypeShort));
        icInt = new IntegerChecker(buildJson(intTypeInt));
        icLong = new IntegerChecker(buildJson(intTypeLong));
    }

    @Test
    void throwsInvalidIntTypeException() {
        assertThrows(IntegerChecker.InvalidIntTypeException.class, () -> new IntegerChecker(buildJson(intTypeInvalid)));
    }

    @Test
    void validatesShortUpperVars() {
        assertEquals(intTypeShort, icShortUpper.getIntType());
        assertEquals(safeLengthShort, icShortUpper.getSafeLength());
    }

    @Test
    void validatesShortVars() {
        assertEquals(intTypeShort, icShort.getIntType());
        assertEquals(safeLengthShort, icShort.getSafeLength());
    }

    @Test
    void validatesShortPos() {
        assertEquals(true, icShort.validates(validShort));
        assertEquals(false, icShort.validates(validInt));
        assertEquals(false, icShort.validates(validLong));
    }

    @Test
    void validatesShortNeg() {
        assertEquals(true, icShort.validates(validShortNeg));
        assertEquals(false, icShort.validates(validIntNeg));
        assertEquals(false, icShort.validates(validLongNeg));
    }

    @Test
    void validatesShortNan() {
        assertEquals(false, icShort.validates(invalidFloat));
        assertEquals(false, icShort.validates(invalidDouble));
        assertEquals(false, icShort.validates(invalidString));
    }

    @Test
    void validatesIntVars() {
        assertEquals(intTypeInt, icInt.getIntType());
        assertEquals(safeLengthInt, icInt.getSafeLength());
    }

    @Test
    void validatesIntPos() {
        assertEquals(true, icInt.validates(validShort));
        assertEquals(true, icInt.validates(validInt));
        assertEquals(false, icInt.validates(validLong));
    }

    @Test
    void validatesIntNeg() {
        assertEquals(true, icInt.validates(validShortNeg));
        assertEquals(true, icInt.validates(validIntNeg));
        assertEquals(false, icInt.validates(validLongNeg));
    }

    @Test
    void validatesIntNan() {
        assertEquals(false, icInt.validates(invalidFloat));
        assertEquals(false, icInt.validates(invalidDouble));
        assertEquals(false, icInt.validates(invalidString));
    }

    @Test
    void validatesLongVars() {
        assertEquals(intTypeLong, icLong.getIntType());
        assertEquals(safeLengthLong, icLong.getSafeLength());
    }

    @Test
    void validatesLongPos() {
        assertEquals(true, icLong.validates(validShort));
        assertEquals(true, icLong.validates(validInt));
        assertEquals(true, icLong.validates(validLong));
    }

    @Test
    void validatesLongNeg() {
        assertEquals(true, icLong.validates(validShortNeg));
        assertEquals(true, icLong.validates(validIntNeg));
        assertEquals(true, icLong.validates(validLongNeg));
    }

    @Test
    void validatesLongNan() {
        assertEquals(false, icLong.validates(invalidFloat));
        assertEquals(false, icLong.validates(invalidDouble));
        assertEquals(false, icLong.validates(invalidString));
    }

    @Test
    void validatesUnspecifiedVars() {
        assertEquals(intTypeInt, icUnspecified.getIntType());
        assertEquals(safeLengthInt, icInt.getSafeLength());
    }

}
