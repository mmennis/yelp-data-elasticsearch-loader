package yelp.data.elasticsearch.loader.data.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import yelp.data.elasticsearch.loader.data.utils.GenericDataProcessor;

import java.util.HashMap;
import java.util.Map;

public class YelpBusinessDataProcessor extends GenericDataProcessor {

  @Override
  public void process() {

    Map<String, Object> jsonObject = getJsonObject();
    Map<String, Object> locationMap = new HashMap<>();

    locationMap.put("lat", jsonObject.get("latitude"));
    locationMap.put("lon", jsonObject.get("longitude"));
    jsonObject.put("location", locationMap);
    jsonObject.remove("latitude");
    jsonObject.remove("longitude");

  }

}
