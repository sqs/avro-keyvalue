package com.blendlabsinc.avrokeyvalue;

/**
 * JSON to flat map encoder
 *
 */
public class KeyValueEncoder
{
    public static java.util.Map<String, String> encode(String s)
    {
        java.util.Map<String, String> encodedMap = new java.util.HashMap();
        encodedMap.put("", s);
        return encodedMap;
    }
}
