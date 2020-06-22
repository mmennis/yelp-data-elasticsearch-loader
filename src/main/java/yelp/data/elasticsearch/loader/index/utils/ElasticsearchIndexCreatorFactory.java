package yelp.data.elasticsearch.loader.index.utils;

import org.elasticsearch.client.RestHighLevelClient;
import yelp.data.elasticsearch.loader.index.creators.YelpBusinessDataIndexCreator;
import yelp.data.elasticsearch.loader.index.creators.YelpReviewDataIndexCreator;
import yelp.data.elasticsearch.loader.index.creators.YelpTipDataIndexCreator;
import yelp.data.elasticsearch.loader.index.creators.YelpUserDataIndexCreator;

import java.util.Properties;

public class ElasticsearchIndexCreatorFactory {

  private RestHighLevelClient _client;

  public ElasticsearchIndexCreatorFactory(RestHighLevelClient client) {
    this._client = client;
  }

  public ElasticsearchIndexCreator getIndexCreator(String dataSeries) {
    ElasticsearchIndexCreator retVal;
    switch(dataSeries) {
      case "businesses":
        retVal = new YelpBusinessDataIndexCreator(dataSeries, this._client);
        break;
      case "users":
        retVal = new YelpUserDataIndexCreator(dataSeries, this._client);
        break;
      case "reviews":
        retVal = new YelpReviewDataIndexCreator(dataSeries, this._client);
        break;
      case "tips":
        retVal = new YelpTipDataIndexCreator(dataSeries, this._client);
        break;
      default:
        retVal = null;
    }
    return retVal;
  }

  public void setClient(RestHighLevelClient _client) {
    this._client = _client;
  }
}
