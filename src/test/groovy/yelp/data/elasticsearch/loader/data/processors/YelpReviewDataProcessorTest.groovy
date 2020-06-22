package yelp.data.elasticsearch.loader.data.processors

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class YelpReviewDataProcessorTest extends Specification {

  String filename = "samples/review.sample.json"
  ClassLoader classLoader = new YelpReviewDataProcessorTest().getClass().getClassLoader()
  File file = new File(classLoader.getResource(filename).getFile())
  String jsonData = FileUtils.readFileToString(file)

  YelpReviewDataProcessor processor

  void setup() {
    processor = new YelpReviewDataProcessor()
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

}
