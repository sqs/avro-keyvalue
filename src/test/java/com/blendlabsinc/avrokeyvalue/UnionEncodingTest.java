package com.blendlabsinc.avrokeyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

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
 * Unit test for Avro union encoding.
 */
public class UnionEncodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public UnionEncodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(UnionEncodingTest.class);
    }

    /**
     * Helpers
     */

    private Map<String, String> write(Schema schema, Object datum) throws IOException {
        DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
        Map<String, String> out = new java.util.HashMap();
        KeyValueEncoder encoder = new KeyValueEncoder(schema, out);
        writer.write(datum, encoder);
        return out;
    }

    /**
     * Test cases
     */

    public void testUnion() throws IOException {
        Schema schema = Schema.parse("[\"string\", \"int\"]");

        Map<String, String> expectedS = new java.util.HashMap();
        expectedS.put("string", "a");
        assertEquals(expectedS, write(schema, "a"));

        Map<String, String> expectedI = new java.util.HashMap();
        expectedI.put("int", "1");
        assertEquals(expectedI, write(schema, 1));
    }

    public void testUnionWithNull() throws IOException {
        Schema schema = Schema.parse("[\"null\", \"string\"]");

        Map<String, String> expectedN = new java.util.HashMap();
        expectedN.put("string", "a");
        assertEquals(expectedN, write(schema, "a"));

        Map<String, String> expectedS = new java.util.HashMap();
        assertEquals(expectedS, write(schema, null));
    }

  public void testUnionWithManyTypes() throws IOException {
    Schema schema = Schema.parse("[\"null\", \"string\", \"int\"]");

    Map<String, String> expectedS = new java.util.HashMap();
    assertEquals(expectedS, write(schema, null));

    Map<String, String> expectedN = new java.util.HashMap();
    expectedN.put("string", "a");
    assertEquals(expectedN, write(schema, "a"));

    Map<String, String> expectedI = new java.util.HashMap();
    expectedI.put("int", "1");
    assertEquals(expectedI, write(schema, 1));
  }
}
