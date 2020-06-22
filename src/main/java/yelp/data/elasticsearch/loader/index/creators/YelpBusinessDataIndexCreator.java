package yelp.data.elasticsearch.loader.index.creators;

import org.elasticsearch.client.RestHighLevelClient;
import yelp.data.elasticsearch.loader.index.utils.ElasticsearchIndexCreator;
import static yelp.data.elasticsearch.loader.index.utils.ElasticsearchTypeMappingUtils.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class YelpBusinessDataIndexCreator extends ElasticsearchIndexCreator {

  private final static Logger LOGGER = Logger.getLogger(YelpBusinessDataIndexCreator.class.getName());

  public YelpBusinessDataIndexCreator(String datasetName,
                                      RestHighLevelClient client) {
    super(datasetName, client);
  }

  @Override
  public void addMapping() {
    LOGGER.info("Creating a mapping for the businesses index.");
    _mapping = new HashMap<>();

    Map<String, Object> properties = new HashMap<>();
    properties.put("business_id", getKeywordMessage());
    properties.put("timestamp", getTimestampMessage());

    properties.put("name", getKeywordMessage());
    properties.put("address", getTextMessage());
    properties.put("city",getKeywordMessage());
    properties.put("state",getKeywordMessage());
    properties.put("postal_code",getKeywordMessage());
    properties.put("location", getGeoPointMessage());
    properties.put("stars", getFloatMessage());
    properties.put("review_count", getIntMessage());
    properties.put("categories", getTextMessage());
    properties.put("hours", getNestedMessage());

    _mapping.put("properties", properties);
  }
}
