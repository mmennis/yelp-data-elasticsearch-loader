package yelp.data.elasticsearch.loader.data.processors;

import yelp.data.elasticsearch.loader.data.utils.GenericDataProcessor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class YelpUserDataProcessor extends GenericDataProcessor {
  @Override
  public void process() {
    Map<String, Object> jsonObject = getJsonObject();

    jsonObject.put("id", jsonObject.get("user_id"));
    jsonObject.put("timestamp", System.currentTimeMillis());

    String friends = (String) jsonObject.get("friends");
    String[] friendsArr = friends.split(",");
    jsonObject.put("friends", friendsArr);
    String elite = (String) jsonObject.get("elite");
    String[] eliteYears = elite.split(",");
    jsonObject.put("elite", eliteYears);
  }
}
