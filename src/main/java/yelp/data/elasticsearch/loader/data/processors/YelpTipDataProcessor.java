package yelp.data.elasticsearch.loader.data.processors;

import yelp.data.elasticsearch.loader.data.utils.GenericDataProcessor;

import java.util.Map;
import java.util.UUID;

public class YelpTipDataProcessor extends GenericDataProcessor {

  @Override
  public void process() {
    UUID uuid = UUID.randomUUID();
    Map<String, Object> jsonObject = getJsonObject();

    jsonObject.put("id", uuid.toString());
    jsonObject.put("timestamp", System.currentTimeMillis());

  }
}
