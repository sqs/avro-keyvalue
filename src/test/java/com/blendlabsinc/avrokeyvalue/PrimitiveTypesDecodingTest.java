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

    public void testString() throws IOException {
        Schema schema = Schema.parse("\"string\"");
        Map<String, Object> in = new java.util.HashMap();
        in.put("", "a");
        assertEquals(new Utf8("a"), decode(schema, in));
    }

}
