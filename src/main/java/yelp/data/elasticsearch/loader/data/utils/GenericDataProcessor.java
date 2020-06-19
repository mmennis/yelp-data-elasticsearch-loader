package yelp.data.elasticsearch.loader.data.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;
import java.util.Map;

public abstract class GenericDataProcessor {

  protected ObjectMapper objectMapper;
  private String jsonString;
  private Map<String, Object> jsonObject;

  public GenericDataProcessor() {
    this.objectMapper = new ObjectMapper();
  }

  public abstract void process();

  public void parseJsonDataString(String jsonData) throws JsonProcessingException {
    this.jsonString = jsonData;
    this.jsonObject = objectMapper.readValue(this.jsonString, new TypeReference<Map<String, Object>>(){});
  }

  public Map<String, Object> getProcessedJson() {
    return null;
  }

  public String getJsonString() {
    return jsonString;
  }

  public Map<String, Object> getJsonObject() {
    return jsonObject;
  }
}
