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
public class EnumEncodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public EnumEncodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(EnumEncodingTest.class);
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

    public void testEnum() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"enum\", \"name\": \"MyEnum\", \"symbols\": [\"A\", \"B\", \"C\"]}");

        Map<String, String> expectedA = new java.util.HashMap();
        expectedA.put("", "A");
        assertEquals(expectedA, write(schema, "A"));

        Map<String, String> expectedB = new java.util.HashMap();
        expectedB.put("", "B");
        assertEquals(expectedB, write(schema, "B"));
    }
}
