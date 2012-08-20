package com.blendlabsinc.avrokeyvalue;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Unit test for Avro union encoding.
 */
public class RecordDecodingTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public RecordDecodingTest(String testName) {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(RecordDecodingTest.class);
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

  private Object decode(Schema schema, Map<String, String> in) throws IOException {
    DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
    Decoder decoder = new KeyValueDecoder(schema, in);
    return reader.read(null, decoder);
  }

    /**
     * Test cases
     */

    public void testRecord() throws IOException {
      Schema schema = Schema.parse("{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"a\", \"type\": \"int\"}]}");

      GenericRecord expected = new GenericData.Record(schema);
      expected.put("a", 1);

      Map<String, String> in = new TreeMap();
      in.put("a", "1");

      assertEquals(expected, decode(schema, in));
    }

  public void testRecordWithNull() throws IOException {
    Schema schema = Schema.parse("{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"a\", \"type\": \"null\"}]}");

    GenericRecord expected = new GenericData.Record(schema);
    expected.put("a", null);

    Map<String, String> in = new TreeMap();

    assertEquals(expected, decode(schema, in));
  }

    public void testRecordWithTwoFields() throws IOException {
      Schema schema = Schema.parse("{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"a\", \"type\": \"string\"}, {\"name\": \"b\", \"type\": \"int\"}]}");

      GenericRecord expected = new GenericData.Record(schema);
      expected.put("a", "aa");
      expected.put("b", 1);

      Map<String, String> in = new TreeMap();
      in.put("a", "aa");
      in.put("b", "1");

      assertEquals(expected, decode(schema, in));
    }

  public void testRecordWithMaps() throws IOException {
    Schema schema = Schema.parse("{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"a\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}, {\"name\": \"b\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}]}");

    Map<String, Object> aMap = new java.util.TreeMap();
    Map<String, Object> bMap = new java.util.TreeMap();
    aMap.put("aa", "aaa");
    bMap.put("bb", "bbb");

    GenericRecord expected = new GenericData.Record(schema);
    expected.put("a", convertStringToUtf8(aMap));
    expected.put("b", convertStringToUtf8(bMap));

    Map<String, String> in = new java.util.TreeMap();
    in.put("a|aa", "aaa");
    in.put("b|bb", "bbb");

    assertEquals(expected, decode(schema, in));
  }

  public void testRecordWithUnion() throws IOException {
    Schema schema = Schema.parse("{\"type\": \"record\", \"name\": \"MyRecord\", \"fields\": [{\"name\": \"a\", \"type\": [\"null\", \"int\", {\"type\": \"map\", \"values\": \"string\"}]}]}");

    Map<String, String> inN = new java.util.TreeMap();
    GenericRecord expectedN = new GenericData.Record(schema);
    expectedN.put("a", null);
    assertEquals(expectedN, decode(schema, inN));

    Map<String, String> inI = new java.util.TreeMap();
    inI.put("a|int", "123");
    GenericRecord expectedI = new GenericData.Record(schema);
    expectedI.put("a", 123);
    assertEquals(expectedI, decode(schema, inI));

    Map<String, String> inM = new java.util.TreeMap();
    inM.put("a|map|aa", "aaa");
    inM.put("a|map|bb", "bbb");
    GenericRecord expectedM = new GenericData.Record(schema);
    Map<String, Object> mapM = new TreeMap<String,Object>();
    mapM.put("aa", "aaa");
    mapM.put("bb", "bbb");
    expectedM.put("a", convertStringToUtf8(mapM));
    assertEquals(expectedM, decode(schema, inM));
  }
}
