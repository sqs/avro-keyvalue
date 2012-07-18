package com.blendlabsinc.avrokeyvalue;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for Avro primitive type encoding.
 */
public class PrimitiveTypeEncodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PrimitiveTypeEncodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(PrimitiveTypeEncodingTest.class);
    }

    public void testString() {
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "a");
        assertEquals(expected, KeyValueEncoder.encode("a"));
    }
}
