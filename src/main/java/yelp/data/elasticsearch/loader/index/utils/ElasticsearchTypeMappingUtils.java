package yelp.data.elasticsearch.loader.index.utils;

import java.util.HashMap;
import java.util.Map;

public class ElasticsearchTypeMappingUtils {

  public static Map<String, Object> getKeywordMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "keyword");
    return message;
  }

  public static Map<String, Object> getTextMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "text");
    return message;
  }

  public static Map<String, Object> getIntMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "int");
    return message;
  }

  public static Map<String, Object> getGeoPointMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "geo_point");
    return message;
  }

  public static Map<String, Object> getBooleanMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "boolean");
    return message;
  }

  public static Map<String, Object> getNestedMessage() {
    Map<String, Object> message = new HashMap<>();
    message.put("type", "nested");
    return message;
  }
}
