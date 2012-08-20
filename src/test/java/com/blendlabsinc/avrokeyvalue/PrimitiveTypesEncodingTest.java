package com.blendlabsinc.avrokeyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

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

    public void testNull() throws IOException {
        Schema schema = Schema.parse("\"null\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "");
        assertEquals(expected, write(schema, null));
    }

    public void testBoolean() throws IOException {
        Schema schema = Schema.parse("\"boolean\"");

        java.util.Map<String, String> expectedT = new java.util.HashMap();
        expectedT.put("", "true");
        assertEquals(expectedT, write(schema, true));

        java.util.Map<String, String> expectedF = new java.util.HashMap();
        expectedF.put("", "false");
        assertEquals(expectedF, write(schema, false));
    }

    public void testInt() throws IOException {
        Schema schema = Schema.parse("\"int\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "1");
        assertEquals(expected, write(schema, new Integer(1)));
    }

    public void testLong() throws IOException {
        Schema schema = Schema.parse("\"long\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "34359738368"); // 2^35
        assertEquals(expected, write(schema, new Long(34359738368L)));
    }

    public void testFloat() throws IOException {
        Schema schema = Schema.parse("\"float\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "1.5");

        assertEquals(expected, write(schema, new Float(1.5)));
    }

    public void testDouble() throws IOException {
        Schema schema = Schema.parse("\"double\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "1.5");

        assertEquals(expected, write(schema, new Double(1.5)));
    }

    public void testString() throws IOException {
        Schema schema = Schema.parse("\"string\"");
        java.util.Map<String, String> expected = new java.util.HashMap();
        expected.put("", "a");
        assertEquals(expected, write(schema, "a"));
    }

    public void testBytes() throws IOException {
      Schema schema = Schema.parse("\"bytes\"");
      java.util.Map<String, String> expected = new java.util.HashMap();
      expected.put("", "a");
      byte[] bytes = {'a'};
      assertEquals(expected, write(schema, ByteBuffer.wrap(bytes)));
    }


}
