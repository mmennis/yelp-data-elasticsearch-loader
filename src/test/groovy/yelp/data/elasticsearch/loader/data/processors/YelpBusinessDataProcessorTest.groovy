package yelp.data.elasticsearch.loader.data.processors

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class YelpBusinessDataProcessorTest extends Specification {

  String filename = "samples/business.sample.json"
  ClassLoader classLoader = new YelpBusinessDataProcessor().getClass().getClassLoader()
  File file = new File(classLoader.getResource(filename).getFile())
  String jsonData = FileUtils.readFileToString(file)

  YelpBusinessDataProcessor processor

  void setup() {
    processor = new YelpBusinessDataProcessor()
  }

  void cleanup() {
  }

  def "empty test"() {
    when:
//      processor.p
      def i = 1 + 1
    then:
      i == 2
  }
}
