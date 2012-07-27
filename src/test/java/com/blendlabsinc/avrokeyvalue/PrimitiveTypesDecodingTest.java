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
import org.apache.avro.io.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.util.Utf8;

/**
 * Unit test for Avro primitive type decoding.
 */
public class PrimitiveTypesDecodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PrimitiveTypesDecodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(PrimitiveTypesDecodingTest.class);
    }

    /**
     * Helpers
     */

    private Object decode(Schema schema, Map<String, Object> in) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
        Decoder decoder = new KeyValueDecoder(schema, in);
        return reader.read(null, decoder);
    }

    /**
     * Test cases
     */

    public void testNull() throws IOException {
        Schema schema = Schema.parse("\"null\"");
        java.util.Map<String, Object> in = new java.util.HashMap();
        in.put("", "");
        assertEquals(null, decode(schema, in));
    }

    public void testBoolean() throws IOException {
        Schema schema = Schema.parse("\"boolean\"");

        java.util.Map<String, Object> inT = new java.util.HashMap();
        inT.put("", "true");
        assertEquals(true, decode(schema, inT));

        java.util.Map<String, Object> inF = new java.util.HashMap();
        inF.put("", "false");
        assertEquals(false, decode(schema, inF));
    }

    public void testInt() throws IOException {
        Schema schema = Schema.parse("\"int\"");
        java.util.Map<String, Object> in = new java.util.HashMap();
        in.put("", "1");
        assertEquals(new Integer(1), decode(schema, in));
    }

    public void testLong() throws IOException {
        Schema schema = Schema.parse("\"long\"");
        java.util.Map<String, Object> in = new java.util.HashMap();
        in.put("", "34359738368"); // 2^35
        assertEquals(new Long(34359738368L), decode(schema, in));
    }

    public void testFloat() throws IOException {
        Schema schema = Schema.parse("\"float\"");
        java.util.Map<String, Object> in = new java.util.HashMap();
        in.put("", "1.5");
        assertEquals(new Float(1.5), decode(schema, in));
    }

    public void testDouble() throws IOException {
        Schema schema = Schema.parse("\"double\"");
        java.util.Map<String, Object> in = new java.util.HashMap();
        in.put("", "1.5");
        assertEquals(new Double(1.5), decode(schema, in));
    }

    public void testString() throws IOException {
        Schema schema = Schema.parse("\"string\"");
        Map<String, Object> in = new java.util.HashMap();
        in.put("", "a");
        assertEquals(new Utf8("a"), decode(schema, in));
    }

    public void testBytes() throws IOException {
        Schema schema = Schema.parse("\"bytes\"");
        // TODO
    }


}
