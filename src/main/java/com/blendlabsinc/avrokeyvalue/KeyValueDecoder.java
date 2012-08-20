package com.blendlabsinc.avrokeyvalue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

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
  private Map<String, String> in;

  Stack<ReorderBuffer> reorderBuffers = new Stack<ReorderBuffer>();
  ReorderBuffer currentReorderBuffer;

  private static class ReorderBuffer {
    public Map<String, Map<String, String>> savedFields = new HashMap<String, Map<String, String>>();
    public Map<String, String> origParser = null;
  }

  private KeyValueDecoder(Symbol root, Map<String, String> in) throws IOException {
    super(root);
    configure(in);
  }

  KeyValueDecoder(Schema schema, Map<String, String> in) throws IOException {
    this(getSymbol(schema), in);
  }

  private static Symbol getSymbol(Schema schema) {
    if (null == schema) {
      throw new NullPointerException("Schema cannot be null!");
    }
    return new JsonGrammarGenerator().generate(schema);
  }

  public KeyValueDecoder configure(Map<String, String> in) throws IOException {
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
    trace("readNull");
    if (in.containsKey(getKeyPathString())) {
      error("null");
    }
  }

  @Override
  public boolean readBoolean() throws IOException {
    advance(Symbol.BOOLEAN);
    String val = (String)in.get(getKeyPathString());
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
    String val = (String)in.get(getKeyPathString());
    return Long.parseLong(val);
  }

  @Override
  public float readFloat() throws IOException {
    advance(Symbol.FLOAT);
    String val = (String)in.get(getKeyPathString());
    return Float.parseFloat(val);
  }

  @Override
  public double readDouble() throws IOException {
    advance(Symbol.DOUBLE);
    String val = (String)in.get(getKeyPathString());
    return Double.parseDouble(val);
  }
    
  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    // trace("readStringUtf8");
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    advance(Symbol.STRING);
    trace("readString");
    if (parser.topSymbol() == Symbol.MAP_KEY_MARKER) {
      advance(Symbol.MAP_KEY_MARKER);
      if (in.isEmpty()) {
        throw error("map-key");
      }

      String result = in.keySet().iterator().next();
      print(" - MAP_KEY: raw=" + result + " (getKeyPathString() = " + getKeyPathString() + ")");
      assert(result != null);

      // Strip the prefix consisting of the current keypath from this key
      if (!getKeyPathString().isEmpty()) {
        String keyPathPrefix = getKeyPathString() + "|";
        assert(result.startsWith(keyPathPrefix));
        result = result.substring(keyPathPrefix.length());
      }

      // Only get the next key, not the entire descending keypath
      if (result.indexOf('|') != -1) {
        result = result.substring(0, result.indexOf('|'));
      }

      pushKeyPathComponent(result);
      return result;
    } else {
      if (in.isEmpty()) {
        throw error("string");
      }
      String val = (String)in.get(getKeyPathString());
      print(" - STRING, map += '" + getKeyPathString() + "' -> '" + in.toString() + "'");
      print("         now in=" + in.toString());
      assert(val != null);
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
    advance(Symbol.ITEM_END);
    return doMapNext(false);
  }

  private long doMapNext(boolean fromReadMapStart) throws IOException {
    if (!fromReadMapStart) {
      Object removed = in.remove(getKeyPathString());
      // assert(removed != null);
      popKeyPathComponent();
    }

    long numLeft = 0;
    for (String key : in.keySet()) {
      if (key.startsWith(getKeyPathString())) numLeft++;
    }

    trace("  doMapNext numLeft=" + numLeft);

    if (numLeft == 0 && !fromReadMapStart) {
      advance(Symbol.MAP_END);
      trace("MAP_END");
    }
    return Math.min(1, numLeft);
  }

  @Override public long skipMap() throws IOException {
    throw new IOException("skipMap not implemented");
  }

  @Override
  public int readIndex() throws IOException {
    advance(Symbol.UNION);
    Symbol.Alternative a = (Symbol.Alternative) parser.popSymbol();

    String label;
    if (in.isEmpty()) {
      label = "null";
    } else {
      label = stripKeyPathSuffix(stripKeyPathPrefix(in.keySet().iterator().next(), getKeyPathString()));
      parser.pushSymbol(Symbol.UNION_END);
    }
    int n = a.findLabel(label);
    if (n < 0)
      throw new AvroTypeException("Unknown union branch " + label);
    parser.pushSymbol(a.getSymbol(n));
    pushKeyPathComponent(label);
    return n;
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
    symbolStr = symbolStr.replace("org.apache.avro.io.parsing.", "");
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
      pushKeyPathComponent(name);
      print(" - doAction(fieldName=" + name + ")");
      if (currentReorderBuffer != null) {
         Map<String, String> savedSubMap = currentReorderBuffer.savedFields.get(name);
         if (savedSubMap != null) {
           currentReorderBuffer.savedFields.remove(name);
           currentReorderBuffer.origParser = in;
           this.in = new HashMap<String, String>(savedSubMap);
           print(" - goto savedSubMap");
           return null;
         }
      }
      if (in.size() > 0) {
         do {
           String nextKey = in.keySet().iterator().next();
           String fn = nextKey.indexOf("|") == -1 ? nextKey : nextKey.substring(0, nextKey.indexOf("|"));
           if (name.equals(fn)) {
             print(" - name (" + name + ") == (" + fn + ")");
             return null;
           } else {
             if (currentReorderBuffer == null) {
               currentReorderBuffer = new ReorderBuffer();
             }
             Map<String, String> subMap = new HashMap<String, String>();
             Iterator it = in.entrySet().iterator();
             while (it.hasNext()) {
               Map.Entry<String, String> entry = (Map.Entry)it.next();
               String keyPrefix = fn + "|";
               print(" - keyPrefix='" + keyPrefix + "', key='" + entry.getKey() + "'");
               if (entry.getKey().startsWith(keyPrefix) || entry.getKey().equals(fn)) {
                 //String keyWithoutPrefix = entry.getKey().substring(keyPrefix.length());
                 print(" - save field '" + fn + "' = " + entry.getValue());
                 subMap.put(/*keyWithoutPrefix*/fn, entry.getValue());
                 it.remove();
               }
             }
             currentReorderBuffer.savedFields.put(fn, subMap);
           }
           print(" - in=" + in);
         } while (in.size() > 0);
         throw new AvroTypeException("Expected field name not found: " + fa.fname);
      }
    } else if (top == Symbol.FIELD_END) {
      print(" - FIELD_END");
      if (currentReorderBuffer != null && currentReorderBuffer.origParser != null) {
         in = currentReorderBuffer.origParser;
         currentReorderBuffer.origParser = null;
      }
      popKeyPathComponent();
    } else if (top == Symbol.RECORD_START) {
      print(" - RECORD_START");
      reorderBuffers.push(currentReorderBuffer);
      currentReorderBuffer = null;
    } else if (top == Symbol.RECORD_END || top == Symbol.UNION_END) {
      print(" - RECORD_END / UNION_END  (top = " + top + ")");
      if (in.size() == 0) {
         if (top == Symbol.RECORD_END) {
           if (currentReorderBuffer != null && !currentReorderBuffer.savedFields.isEmpty()) {
             throw error("Unknown fields: " + currentReorderBuffer.savedFields.keySet());
           }
           currentReorderBuffer = reorderBuffers.pop();
         }
      } else {
         throw error(top == Symbol.RECORD_END ? "record-end" : "union-end");
      }
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

  // otherKeyPath = "a|b|c", partialKeyPathToRemove="a|b" -> "c"
  private String stripKeyPathPrefix(String otherKeyPath, String partialKeyPathToRemove) {
    if (otherKeyPath.startsWith(getKeyPathString()) == false) {
      trace("otherKeyPath '" + otherKeyPath + "' doesnt start with current key path");
    }
    assert(otherKeyPath.startsWith(getKeyPathString()));
    if (otherKeyPath.indexOf("|") == -1) {
      return otherKeyPath;
    } else {
      String keyPathPrefix = getKeyPathString() + "|";
      return otherKeyPath.substring(keyPathPrefix.length());
    }
  }

  // "a|b|c" -> "a"
  private String stripKeyPathSuffix(String keyPath) {
    return keyPath.indexOf("|") == -1 ? keyPath : keyPath.substring(0, keyPath.indexOf("|"));
  }
}

