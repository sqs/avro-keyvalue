package com.blendlabsinc.avrokeyvalue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ParsingDecoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.io.parsing.ValidatingGrammarGenerator;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;

public class KeyValueDecoder extends ParsingDecoder implements Parser.ActionHandler {
  
  private Schema schema;
  private Map<String, Object> in;

  private KeyValueDecoder(Symbol root, Map<String, Object> in) throws IOException {
    super(root);
    configure(in);
  }

  KeyValueDecoder(Schema schema, Map<String, Object> in) throws IOException {
    this(getSymbol(schema), in);
  }

  private static Symbol getSymbol(Schema schema) {
    if (null == schema) {
      throw new NullPointerException("Schema cannot be null!");
    }
    return new ValidatingGrammarGenerator().generate(schema);
  }

  public KeyValueDecoder configure(Map<String, Object> in) throws IOException {
    if (null == in) {
      throw new NullPointerException("Map to read from cannot be null!");
    }
    parser.reset();
    this.in = new java.util.TreeMap(in);
    return this;
  }

  private void advance(Symbol symbol) throws IOException {
    this.parser.processTrailingImplicitActions();
    parser.advance(symbol);
  }

  /////////////////////////////////////////////////////////////////////////////


  @Override public void readNull() throws IOException {
    if (!((String)in.get("")).equals("")) {
      error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    String val = (String)in.get("");
    if (val == Boolean.toString(true) || val == Boolean.toString(false)) {
      return val == Boolean.toString(true);
    } else {
      throw error("boolean");
    }
  }

  @Override
  public int readInt() throws IOException {
    advance(Symbol.INT);
    String val = (String)in.get(getKeyPathString());
    trace("readInt('" + val + "')");
    return Integer.parseInt(val);
  }
    
  @Override
  public long readLong() throws IOException {
    advance(Symbol.LONG);
    String val = (String)in.get("");
    return Long.parseLong(val);
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    String val = (String)in.get("");
    return Float.parseFloat(val);
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    String val = (String)in.get("");
    return Double.parseDouble(val);
  }
    
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    // trace("readStringUtf8");
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    trace("readString");
    Boolean shouldReadMapKey = parser.topSymbol() == Symbol.MAP_START ||
            (parser.topSymbol() instanceof Symbol.Repeater &&
                    ((Symbol.Repeater)parser.topSymbol()).end == Symbol.MAP_END);
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      advance(Symbol.MAP_KEY_MARKER);
      advance(Symbol.STRING);
      if (in.isEmpty()) {
        throw error("map-key");
      }
      String result = in.keySet().iterator().next();
      print(" - MAP_KEY: raw=" + result + " (getKeyPathString() = " + getKeyPathString() + ")");
      if (!getKeyPathString().isEmpty()) result = result.replace("|" + getKeyPathString(), "");
      if (result.indexOf('|') != -1) result = result.substring(0, result.indexOf('|'));
      pushKeyPathComponent(result);
      return result;
    } else {
      advance(Symbol.STRING);
      if (in.isEmpty()) {
        throw error("string");
      }
      String val = (String)in.get(getKeyPathString());
      print(" - STRING, map += " + getKeyPathString() + " -> " + in.toString());
      print("         now in = " + in.toString());
      return val;
    }
  }

  @Override
  public void skipString() throws IOException {
    readString();
  }

  @Override public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return null;
  }

  @Override public void skipBytes() throws IOException {
  
  }

  @Override public void readFixed(byte[] bytes, int start, int length) throws IOException {
  
  }

  @Override public void skipFixed() throws IOException {
  }

  @Override public void skipFixed(int length) throws IOException {
  
  }

  @Override public int readEnum() throws IOException {
    return 0;
  }

  @Override public long readArrayStart() throws IOException {
    return 0;
  }

  @Override public long arrayNext() throws IOException {
    return 0;
  }

  @Override public long skipArray() throws IOException {
    return 0;
  }

  @Override public long readMapStart() throws IOException {
    trace("readMapStart");
    advance(Symbol.MAP_START);
    return doMapNext(true);
  }

  @Override public long mapNext() throws IOException {
    trace("mapNext");
    return doMapNext(false);
  }

  private long doMapNext(boolean fromReadMapStart) throws IOException {
    if (!fromReadMapStart) {
      Object removed = in.remove(getKeyPathString());
      assert(removed != null);
      popKeyPathComponent();
    }

    long numLeft = 0;
    for (String key : in.keySet()) {
      if (key.startsWith(getKeyPathString())) numLeft++;
    }

    trace("  doMapNext numLeft=" + numLeft);

    if (numLeft > 0) {
      parser.pushSymbol(Symbol.MAP_KEY_MARKER);
    }

    if (numLeft == 0 && !fromReadMapStart) {
      advance(Symbol.MAP_END);
      trace("MAP_END");
    }
    return Math.min(1, numLeft);
  }

  @Override public long skipMap() throws IOException {
    throw new IOException("skipMap not implemented");
  }

  @Override public int readIndex() throws IOException {
    return 0;
  }

  private AvroTypeException error(String type) {
    return new AvroTypeException(
      "Expected " + type + ". Got " + in.toString()
    );
  }

  private void trace(String s) {
    String symbolStr = (parser.topSymbol() instanceof Symbol.Repeater) ?
            "Repeater:"+((Symbol.Repeater)parser.topSymbol()).end.toString() :
            parser.topSymbol().toString();
    System.out.println(s + ":\t topSymbol=" + symbolStr + ", keyPath=" + getKeyPathString() + ", in=" + in.toString());
  }

  private void print(String s) {
    System.out.println(s);
  }

  @Override
  public Symbol doAction(Symbol input, Symbol top) throws IOException {
    trace("doAction(input=" + input + " top=" + top +")");
    if (top instanceof Symbol.FieldAdjustAction) {
      Symbol.FieldAdjustAction fa = (Symbol.FieldAdjustAction) top;
      String name = fa.fname;
      // if (currentReorderBuffer != null) {
      //   List<JsonElement> node = currentReorderBuffer.savedFields.get(name);
      //   if (node != null) {
      //     currentReorderBuffer.savedFields.remove(name);
      //     currentReorderBuffer.origParser = in;
      //     in = makeParser(node);
      //     return null;
      //   }
      // }
      print(" - doAction(fieldName=" + name);
      // if (in.getCurrentToken() == JsonToken.FIELD_NAME) {
      //   do {
      //     String fn = in.getText();
      //     in.nextToken();
      //     if (name.equals(fn)) {
      //       return null;
      //     } else {
      //       if (currentReorderBuffer == null) {
      //         currentReorderBuffer = new ReorderBuffer();
      //       }
      //       currentReorderBuffer.savedFields.put(fn, getVaueAsTree(in));
      //     }
      //   } while (in.getCurrentToken() == JsonToken.FIELD_NAME);
      //   throw new AvroTypeException("Expected field name not found: " + fa.fname);
      // }
    } else if (top == Symbol.FIELD_END) {
      print(" - FIELD_END");
      // if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
      //   in = currentReorderBuffer.origParser;
      //   currentReorderBuffer.origParser = null;
      // }
    } else if (top == Symbol.RECORD_START) {
      print(" - RECORD_START");
      // if (in.getCurrentToken() == JsonToken.START_OBJECT) {
      //   in.nextToken();
      //   reorderBuffers.push(currentReorderBuffer);
      //   currentReorderBuffer = null;
      // } else {
      //   throw error("record-start");
      // }
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      print(" - RECORD_END / UNION_END  (top = " + top + ")");
      // if (in.getCurrentToken() == JsonToken.END_OBJECT) {
      //   in.nextToken();
      //   if (top == Symbol.RECORD_END) {
      //     if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
      //       throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
      //     }
      //     currentReorderBuffer = reorderBuffers.pop();
      //   }
      // } else {
      //   throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
      // }
    } else {
      throw new AvroTypeException("Unknown action symbol " + top);
    }
    return null;
  }

  /////////////////////////////////////////////////////////////////////////////
  // Key path

  private java.util.ArrayList<String> keyPath = new java.util.ArrayList();

  private void pushKeyPathComponent(String component) {
    if (component.contains("|")) print("\n\n\n !!!!!! WARN: key path component contains |\n\n\n");
    if (component.isEmpty()) print(" WARN: pushKeyPathComponent() of empty string");
    keyPath.add(component);
    print(" > pushKeyPathComponent('" + component + "')\tnow keyPath = '" + getKeyPathString() + "'");
  }

  private void popKeyPathComponent() {
    if (keyPath.size() == 0) {
      trace(" !! popKeyPathComponent() attempted, but keyPath is empty");
    }
    keyPath.remove(keyPath.size() - 1);
    print(" > popKeyPathComponent()\tnow keyPath = '" + getKeyPathString() + "'");
  }

  private String getKeyPathString() {
    return StringUtils.join(keyPath, '|');
  }
}

