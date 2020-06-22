package yelp.data.elasticsearch.loader.data.processors

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class YelpBusinessDataProcessorTest extends Specification {

  String filename = "samples/business.sample.json"
  ClassLoader classLoader = new YelpBusinessDataProcessorTest().getClass().getClassLoader()
  File file = new File(classLoader.getResource(filename).getFile())
  String jsonData = FileUtils.readFileToString(file)

  YelpBusinessDataProcessor processor

  void setup() {
    processor = new YelpBusinessDataProcessor()
  }

  def "Processor must add an id field to the new map object"() {
    when:
      processor.parseJsonDataString(jsonData)
      processor.process()
    then:
      processor.getJsonObject().containsKey("id")
  }

  def "Processor must add a timestamp field"() {
    when:
    processor.parseJsonDataString(jsonData)
    processor.process()
    then:
    processor.getJsonObject().containsKey("timestamp")
  }

  def "processor should consolidate lat and long data into a location map"() {
    when:
      processor.parseJsonDataString(jsonData)
      processor.process()
    then:
      processor.getJsonObject().containsKey("location")
      processor.getJsonObject().get("location") instanceof Map<String, Object>
      !processor.getJsonObject().containsKey("latitude")
      !processor.getJsonObject().containsKey("longitude")
  }
}
