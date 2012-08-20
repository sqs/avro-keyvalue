package com.blendlabsinc.avrokeyvalue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

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
public class MapDecodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MapDecodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(MapDecodingTest.class);
    }

    /**
     * Helpers
     */

    private Object convertUtf8ToString(Map<Utf8, Object> map) throws IOException {
      Map<String, Object> stringMap = new java.util.TreeMap();
      for (Map.Entry<Utf8, Object> entry : map.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof Utf8) {
          value = ((CharSequence)value).toString();
        } else if (value instanceof Map<?, ?>) {
          value = convertUtf8ToString((Map<Utf8, Object>)value);
        }
        stringMap.put(entry.getKey().toString(), value);
      }
      return stringMap;
    }

    private Object convertStringToUtf8(Map<String, Object> map) throws IOException {
      Map<Utf8, Object> stringMap = new java.util.TreeMap();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof String) {
          value = new Utf8((String)value);
        } else if (value instanceof Map<?, ?>) {
          value = convertStringToUtf8((Map<String, Object>)value);
        }
        stringMap.put(new Utf8(entry.getKey()), value);
      }
      return stringMap;
    }

    private Object decode(Schema schema, Map<String, Object> in) throws IOException {
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      Decoder decoder = new KeyValueDecoder(schema, in);
      return reader.read(null, decoder);
    }

    /**
     * Test cases
     */

    public void testMap() throws IOException {
        Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"int\"}");

        Map<String, Object> in = new java.util.TreeMap();
        in.put("a", "1");
        in.put("b", "2");

        Map<Utf8, Object> expected = new java.util.TreeMap();
        expected.put(new Utf8("a"), 1);
        expected.put(new Utf8("b"), 2);

        assertEquals(expected, decode(schema, in));
    }

    public void testBigMap() throws IOException {
         Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
         Map<String, Object> in = new java.util.TreeMap();
         in.put("a", "aa");
         in.put("b", "bb");
         in.put("c", "cc");
         in.put("d", "dd");

         assertEquals(convertStringToUtf8(in), decode(schema, in));
    }

    public void testEmptyMap() throws IOException {
         Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
         Map<String, Object> in = new java.util.TreeMap();
         assertEquals(convertStringToUtf8(in), decode(schema, in));
    }

    public void testMapWithEmptyKey() throws IOException {
         Schema schema = Schema.parse("{\"type\": \"map\", \"values\": \"string\"}");
         Map<String, Object> in = new java.util.TreeMap();
         in.put("", "a");
         assertEquals(convertStringToUtf8(in), decode(schema, in));
    }

    public void testNestedMap() throws IOException {
         Schema schema = Schema.parse("{\"type\": \"map\", \"values\": {\"type\": \"map\", \"values\": \"string\"}}");
         Map<String, Map<String, String>> nestedMap = new java.util.TreeMap();
         Map<String, String> mapA = new java.util.TreeMap(); mapA.put("aa", "aaa");
         Map<String, String> mapB = new java.util.TreeMap(); mapB.put("bb", "bbb");
         nestedMap.put("a", mapA);
         nestedMap.put("b", mapB);

         Map<String, Object> in = new java.util.TreeMap();
         in.put("a|aa", "aaa");
         in.put("b|bb", "bbb");

         Map<String, Object> nestedMap2 = new java.util.TreeMap(nestedMap);

         assertEquals(convertStringToUtf8(nestedMap2), decode(schema, in));
    }

    public void testDeeplyNestedMap() throws IOException {
         Schema schema = Schema.parse("{\"type\": \"map\", \"values\": {\"type\": \"map\", \"values\": {\"type\": \"map\", \"values\": \"string\"}}}");
         Map<String, Map<String, Map<String, String>>> nestedMap = new java.util.TreeMap();
         Map<String, String> mapAA = new java.util.TreeMap(); mapAA.put("aaa", "aaaa");
         Map<String, String> mapBB = new java.util.TreeMap(); mapBB.put("bbb", "bbbb");
         Map<String, Map<String, String>> mapA = new java.util.TreeMap(); mapA.put("aa", mapAA);
         Map<String, Map<String, String>> mapB = new java.util.TreeMap(); mapB.put("bb", mapBB);
         nestedMap.put("a", mapA);
         nestedMap.put("b", mapB);

         Map<String, Object> in = new java.util.TreeMap();
         in.put("a|aa|aaa", "aaaa");
         in.put("b|bb|bbb", "bbbb");

         Map<String, Object> nestedMap2 = new java.util.TreeMap(nestedMap);

         assertEquals(convertStringToUtf8(nestedMap2), decode(schema, in));
    }

}
