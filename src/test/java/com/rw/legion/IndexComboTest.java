package com.rw.legion;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IndexComboTest {
    private IndexCombo ic;

    @BeforeEach
    void setUp() {
        ic = new IndexCombo();
    }

    @Test
    void addIndexTest() {
        assertEquals(0, ic.getHashMap().size());
        String index = "foo";
        String value = "bar";
        ic.addIndex(index, value);
        assertEquals(1, ic.getHashMap().size());
    }

    @Test
    void getValueTest() {
        String index = "foo";
        String value = "bar";
        ic.addIndex(index, value);
        assertEquals(ic.getHashMap().get(index), ic.getValue(index));
    }

    @Test
    void getHashMapTest() {
        HashMap<String, String> hm = new HashMap<String, String>();
        assertEquals(hm, ic.getHashMap());
    }

    @Test
    void hashCodeTest() {
        String index = "foo";
        String value = "bar";
        IndexCombo i = new IndexCombo();
        assertEquals(i.getHashMap().hashCode(), ic.hashCode(), "Hash codes for different instances of same contents");
        i.addIndex(index, value);
        assertNotEquals(i.getHashMap().hashCode(), ic.hashCode(), "Hash codes for different instances of different contents");
        ic.addIndex(index, value);
        assertEquals(i.getHashMap().hashCode(), ic.hashCode(), "Hash codes for different instances");
    }

    @Test
    void equalsTest() {
        Object o = new Object();
        assertEquals(false, ic.equals(o), "Not an IndexCombo Object");
        assertEquals(true, ic.equals(ic), "Self-equality");
        IndexCombo i = new IndexCombo();
        assertEquals(true, ic.equals(i), "Same contents");
        IndexCombo c = new IndexCombo();
        c.addIndex("foo", "bar");
        assertEquals(false, ic.equals(c), "Different contents");
    }

}