package com.blendlabsinc.avrokeyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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
 * Unit test for Avro primitive type encoding.
 */
public class SerdesTests extends TestCase {
  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public SerdesTests(String testName) {
    super( testName );
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(SerdesTests.class);
  }

  /**
   * Helpers
   */

  private Object convertStringToUtf8(Map<String, ?> map) throws IOException {
    Map<Utf8, Object> stringMap = new java.util.TreeMap();
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      Object value = entry.getValue();
      if (value instanceof String) {
        value = new Utf8((String)value);
      } else if (value instanceof Map<?, ?>) {
        value = convertStringToUtf8((Map<String, ?>)value);
      }
      stringMap.put(new Utf8(entry.getKey()), value);
    }
    return stringMap;
  }

  private void assertSerdesIsIdentity(String schemaString, Object rec) throws IOException {
    Schema schema = (new Schema.Parser()).parse(schemaString);
    assertEquals(rec, decode(schema, encode(schema, rec)));
  }

  private Map<String, String> encode(Schema schema, Object datum) throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
    Map<String, String> out = new java.util.HashMap();
    KeyValueEncoder encoder = new KeyValueEncoder(schema, out);
    writer.write(datum, encoder);
    return out;
  }

  private Object decode(Schema schema, Map<String, String> in) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Decoder decoder = new KeyValueDecoder(schema, in);
    return reader.read(null, decoder);
  }

  /**
   * Test cases
   */

  public void testNull() throws IOException {
    assertSerdesIsIdentity("\"null\"", null);
  }

  public void testString() throws IOException {
    assertSerdesIsIdentity("\"string\"", new Utf8("abc"));
  }

  public void testInt() throws IOException {
    assertSerdesIsIdentity("\"int\"", 123);
  }

  public void testBoolean() throws IOException {
    assertSerdesIsIdentity("\"boolean\"", true);
  }
}
