package yelp.data.elasticsearch.loader.data.utils;

import yelp.data.elasticsearch.loader.data.processors.YelpBusinessDataProcessor;
import yelp.data.elasticsearch.loader.data.processors.YelpReviewDataProcessor;
import yelp.data.elasticsearch.loader.data.processors.YelpTipDataProcessor;
import yelp.data.elasticsearch.loader.data.processors.YelpUserDataProcessor;

public class DataProcessorFactory {

  public static GenericDataProcessor getDataProcessor(String dataSeries) {
    GenericDataProcessor retVal;
    switch(dataSeries) {
      case "businesses":
        retVal = new YelpBusinessDataProcessor();
        break;
      case "users":
        retVal = new YelpUserDataProcessor();
        break;
      case "reviews":
        retVal = new YelpReviewDataProcessor();
        break;
      case "tips":
        retVal = new YelpTipDataProcessor();
        break;
      default:
        retVal = null;
    }
    return retVal;
  }
}
