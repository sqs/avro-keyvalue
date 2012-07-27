package com.blendlabsinc.avrokeyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.io.EncoderFactory;

/**
 * Unit test for Avro map encoding.
 */
public class MapEncodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MapEncodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(MapEncodingTest.class);
    }

    /**
     * Helpers
     */

    private java.util.Map<String, String> write(Schema schema, Object datum) throws IOException {
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
        java.util.Map<String, String> out = new java.util.HashMap();
        KeyValueEncoder encoder = new KeyValueEncoder(schema, out);
        writer.write(datum, encoder);
        return out;
    }

    /**
     * Test cases
     */

    public void testMap() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("a", "aa");
        expected.put("b", "bb");
        assertEquals(expected, write(schema, expected));
    }

    public void testEmptyMap() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
        java.util.Map<String, String> expected = new java.util.HashMap();
        assertEquals(expected, write(schema, expected));
    }

    public void testMapWithEmptyKey() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "a");
        assertEquals(expected, write(schema, expected));
    }

    public void testNestedMap() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"map\", \"values\": {\"type\": \"map\", \"values\": \"string\"}}");
        java.util.Map<String, java.util.Map<String, String>> nestedMap = new java.util.HashMap();
        java.util.Map<String, String> mapA = new java.util.HashMap(); mapA.put("aa", "aaa");
        java.util.Map<String, String> mapB = new java.util.HashMap(); mapB.put("bb", "bbb");
        nestedMap.put("a", mapA);
        nestedMap.put("b", mapB);

        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("a|aa", "aaa");
        expected.put("b|bb", "bbb");

        assertEquals(expected, write(schema, nestedMap));
    }
}
