package yelp.data.elasticsearch.loader.data.utils;

import org.elasticsearch.client.RestHighLevelClient;

import java.io.File;

public class ElasticsearchIndexPopulatorFactory {

  private RestHighLevelClient _client;
  private File _dataHomeDirectory;

  public ElasticsearchIndexPopulatorFactory(RestHighLevelClient client, File dataDirectory) {
    this._client = client;
    this._dataHomeDirectory = dataDirectory;
  }

  public ElasticsearchIndexPopulator getIndexPopulator(String dataSeries){
    ElasticsearchIndexPopulator retVal;
    File dataFile;
    switch (dataSeries) {
      case "businesses":
        dataFile = new File(this._dataHomeDirectory, "yelp_academic_dataset_business.json");
        retVal = new ElasticsearchIndexPopulator("businesses", "businesses", dataFile, _client);
        break;
      case "users":
        dataFile = new File(this._dataHomeDirectory, "yelp_academic_dataset_user.json");
        retVal = new ElasticsearchIndexPopulator("users", "users", dataFile, _client);
        break;
      case "reviews":
        dataFile = new File(this._dataHomeDirectory, "yelp_academic_dataset_review.json");
        retVal = new ElasticsearchIndexPopulator("reviews", "reviews", dataFile, _client);
        break;
      case "tips":
        dataFile = new File(this._dataHomeDirectory, "yelp_academic_dataset_tip.json");
        retVal = new ElasticsearchIndexPopulator("tips", "tips", dataFile, _client);
        break;
      default:
        retVal = null;
        break;
    }
    return retVal;
  }
}
