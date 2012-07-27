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
 * Unit test for Avro primitive type encoding.
 */
public class PrimitiveTypesEncodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PrimitiveTypesEncodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(PrimitiveTypesEncodingTest.class);
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

    public void testString() throws IOException {
        Schema schema = Schema.parse("\"string\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "a");

        assertEquals(expected, write(schema, "a"));
    }
}
