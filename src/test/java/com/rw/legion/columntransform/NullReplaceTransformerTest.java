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

}
