package yelp.data.elasticsearch.loader.data.utils

import com.fasterxml.jackson.core.JsonParseException
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class GenericDataProcessorTest extends Specification {

  String filename = "samples/business.sample.json"
  ClassLoader classLoader = new GenericDataProcessorTest().getClass().getClassLoader()
  File file = new File(classLoader.getResource(filename).getFile())
  String jsonData = FileUtils.readFileToString(file)
  GenericDataProcessor processor

  void setup() {
    processor = new GenericDataProcessor() {
      @Override
      void process() {
        Map<String, Object> map = this.getJsonObject()
        map.put("testKey", "testValue")
//        System.out.println(this.getJsonObject());
      }
    }
  }

  def "processor can load up and parse the json string"() {
    when:
      processor.parseJsonDataString(jsonData)
      processor.process()

    then:
      processor.getJsonObject().keySet().contains("testKey")
      processor.getJsonObject().get("testKey").equals("testValue")
  }

  def "processor creates a valid nested Map object"() {
    when:
      processor.parseJsonDataString(jsonData)
      processor.process()
      Map<String, Object> jsonObject = processor.getJsonObject()
    then:
      jsonObject.containsKey("attributes")
      jsonObject.get("attributes") instanceof Map<String, Object>
  }

  def "processor fails if json string is null"() {
    setup:
      String jsonString = null
    when:
      processor.parseJsonDataString(jsonString)
    then:
      IllegalArgumentException iae = thrown()
      iae.getMessage() == 'argument "content" is null'
  }

  def "processor fails if json string is malformed"() {
    setup:
      String jsonString = "{\"key1\": \"value1\" \"key2\": \"value2\"}"
    when:
      processor.parseJsonDataString(jsonString)
    then:
      JsonParseException jpe = thrown()
      jpe.getMessage().startsWith("Unexpected character")
  }
}
