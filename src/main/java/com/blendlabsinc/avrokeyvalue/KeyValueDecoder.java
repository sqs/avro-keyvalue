package com.blendlabsinc.avrokeyvalue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.parsing.JsonGrammarGenerator;
import org.apache.avro.io.parsing.Parser;
import org.apache.avro.io.parsing.Symbol;
import org.apache.avro.util.Utf8;

public class KeyValueDecoder extends Decoder {
  
  private Schema schema;
  private Map<String, Object> in;

  KeyValueDecoder(Schema schema, Map<String, Object> in) throws IOException {
    this.schema = schema;
    this.in = in;
  }

  @Override
  public void readNull() throws IOException {
  }

  @Override
  public boolean readBoolean() throws IOException {
    return false;
  }

  @Override
  public int readInt() throws IOException {
    return 0;
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override
  public float readFloat() throws IOException {
    return 0;
  }

  @Override
  public double readDouble() throws IOException {
    return 0;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    return new Utf8(readString());
  }

  @Override
  public String readString() throws IOException {
    return (String)in.get("");
  }

  @Override
  public void skipString() throws IOException {
  
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return null;
  }

  @Override
  public void skipBytes() throws IOException {
  
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
  
  }

  @Override
  public void skipFixed(int length) throws IOException {
  
  }

  @Override
  public int readEnum() throws IOException {
    return 0;
  }

  @Override
  public long readArrayStart() throws IOException {
    return 0;
  }

  @Override
  public long arrayNext() throws IOException {
    return 0;
  }

  @Override
  public long skipArray() throws IOException {
    return 0;
  }

  @Override
  public long readMapStart() throws IOException {
    return 0;
  }

  @Override
  public long mapNext() throws IOException {
    return 0;
  }

  @Override
  public long skipMap() throws IOException {
    return 0;
  }

  @Override
  public int readIndex() throws IOException {
    return 0;
  }

  private AvroTypeException error(String type) {
    return new AvroTypeException(
      "Expected " + type + ". Got " + in.toString()
    );
  }
}

