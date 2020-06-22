package yelp.data.elasticsearch.loader;

import org.elasticsearch.client.RestHighLevelClient;
import yelp.data.elasticsearch.loader.data.utils.ElasticsearchIndexPopulatorFactory;
import yelp.data.elasticsearch.loader.index.utils.ElasticsearchIndexCreator;
import yelp.data.elasticsearch.loader.index.utils.ElasticsearchIndexCreatorFactory;
import yelp.data.elasticsearch.loader.data.utils.ElasticsearchIndexPopulator;

import java.io.File;
import java.util.logging.Logger;

public class YelpDataLoader {

  private final static Logger LOGGER = Logger.getLogger(YelpDataLoader.class.getName());

  private File _datasetDirectory;
  private RestHighLevelClient _client;
  private ElasticsearchIndexCreatorFactory _indexCreatorFactory;
  ElasticsearchIndexPopulatorFactory _indexPopulatorFactory;

  private final static String[] indexNames = {"businesses", "users", "reviews", "tips"};

  public YelpDataLoader(File datasetDirectory, RestHighLevelClient client) {
    this._datasetDirectory = datasetDirectory;
    this._client = client;
    this._indexCreatorFactory = new ElasticsearchIndexCreatorFactory(this._client);
    this._indexPopulatorFactory = new ElasticsearchIndexPopulatorFactory(this._client, datasetDirectory);
  }

  public void execute() throws Exception {
    if ( getDatasetDirectory() == null ) {
      LOGGER.severe("The directory location for the yelp datasets is missing");
      throw new Exception("Missing directory location for yelp datasets");
    }
    for(String datasetName: indexNames) {
      ElasticsearchIndexCreator indexCreator = _indexCreatorFactory.getIndexCreator(datasetName);
      indexCreator.addMapping();
      indexCreator.createIndex();
      ElasticsearchIndexPopulator indexPopulator = _indexPopulatorFactory.getIndexPopulator(datasetName);
      indexPopulator.populateIndex();
    }
  }

  public File getDatasetDirectory() {
    return _datasetDirectory;
  }

  public void setDatasetDirectory(File datasetDirectory) {
    this._datasetDirectory = datasetDirectory;
  }
}
