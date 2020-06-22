package yelp.data.elasticsearch.loader.index.creators;

import org.elasticsearch.client.RestHighLevelClient;
import yelp.data.elasticsearch.loader.index.utils.ElasticsearchIndexCreator;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static yelp.data.elasticsearch.loader.index.utils.ElasticsearchTypeMappingUtils.*;

public class YelpReviewDataIndexCreator extends ElasticsearchIndexCreator {

  private final static Logger LOGGER = Logger.getLogger(YelpReviewDataIndexCreator.class.getName());

  public YelpReviewDataIndexCreator(String datasetName, RestHighLevelClient client) {
    super(datasetName, client);
  }

  @Override
  public void addMapping() {
    LOGGER.info("Creating a mapping for the users index.");
    _mapping = new HashMap<>();
    Map<String, Object> properties = new HashMap<>();
    properties.put("review_id", getKeywordMessage());
    properties.put("timestamp", getTimestampMessage());

    properties.put("user_id", getKeywordMessage());
    properties.put("business_id", getKeywordMessage());
    properties.put("date", getDateMessage());
    properties.put("text", getTextMessage());

    _mapping.put("properties", properties);
  }
}
