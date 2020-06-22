package yelp.data.elasticsearch.loader.data.processors;

import yelp.data.elasticsearch.loader.data.utils.GenericDataProcessor;

import java.util.Map;

public class YelpReviewDataProcessor extends GenericDataProcessor {

  @Override
  public void process() {
    Map<String, Object> jsonObject = getJsonObject();

    jsonObject.put("id", jsonObject.get("review_id"));
    jsonObject.put("timestamp", System.currentTimeMillis());
  }
}
