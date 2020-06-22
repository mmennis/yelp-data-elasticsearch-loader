package yelp.data.elasticsearch.loader.data.processors

import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.lang.reflect.Array

class YelpUserDataProcessorTest extends  Specification {

  String filename = "samples/user.sample.json"
  ClassLoader classLoader = new YelpUserDataProcessorTest().getClass().getClassLoader()
  File file = new File(classLoader.getResource(filename).getFile())
  String jsonData = FileUtils.readFileToString(file)

  YelpUserDataProcessor processor

  void setup() {
    processor = new YelpUserDataProcessor()
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

  def "Processor should replace friends string with an araay"() {
    when:
    processor.parseJsonDataString(jsonData)
    processor.process()
    then:
    processor.getJsonObject().containsKey("friends")
    processor.getJsonObject().get("friends") instanceof String[]
  }

  def "Processor should replace string with array"() {
    when:
    processor.parseJsonDataString(jsonData)
    processor.process()
    then:
    processor.getJsonObject().containsKey("elite")
    processor.getJsonObject().get("elite") instanceof String[]
  }
}
