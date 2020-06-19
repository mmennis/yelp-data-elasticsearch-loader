package yelp.data.elasticsearch.loader.index.utils;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public abstract class ElasticsearchIndexCreator {

  private final static Logger LOGGER = Logger.getLogger(ElasticsearchIndexCreator.class.getName());

  private RestHighLevelClient _client;
  private String _datasetName;
  private int _shardCount;
  private int _replicaCount;
  protected Map<String, Object> _mapping;

  public ElasticsearchIndexCreator(RestHighLevelClient client, String datasetName) {
    this._datasetName = datasetName;
    this._client = client;
  }

  public abstract void addMapping(Map<String, Object> mapping);

  public void createIndex() throws Exception {
    CreateIndexRequest request = new CreateIndexRequest((this._datasetName));
    Settings.Builder settingsBuilder = Settings.builder()
        .put("index.number_of_shards", this._shardCount)
        .put("index.number_of_replicas", this._replicaCount);
    request.settings(settingsBuilder);
    if (_mapping == null) {
      throw new Exception("Unexpect state: Index mapping for " + this._datasetName + " is missing");
    }
    request.mapping(_mapping);
//    request.validate();
//    request.toString();
    try {
      LOGGER.info("Preparing to create index " + this._datasetName +
            " " + getShardCount() + ", " + getReplicaCount() + " replicas.");
      CreateIndexResponse response = this._client.indices().
          create(request, RequestOptions.DEFAULT);
      LOGGER.info("Create index response id: " + response.index());
    } catch (IOException e) {
      LOGGER.severe("Exception thrown in creation of index: " + e.getLocalizedMessage());
      e.printStackTrace();
    } finally {
      LOGGER.info("Create index call complete");
    }

  }

  public int getShardCount() {
    return _shardCount;
  }

  public void setShardCount(int _shardCount) {
    this._shardCount = _shardCount;
  }

  public int getReplicaCount() {
    return _replicaCount;
  }

  public void setReplicaCount(int _replicaCount) {
    this._replicaCount = _replicaCount;
  }

  protected void setMapping(Map<String, Object> mapping) { this._mapping = mapping; }

}
