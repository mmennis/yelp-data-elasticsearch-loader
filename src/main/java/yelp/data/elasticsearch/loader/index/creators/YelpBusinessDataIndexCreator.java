package yelp.data.elasticsearch.loader.index.creators;

import org.elasticsearch.client.RestHighLevelClient;
import yelp.data.elasticsearch.loader.index.utils.ElasticsearchIndexCreator;
import static yelp.data.elasticsearch.loader.index.utils.ElasticsearchTypeMappingUtils.*;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class YelpBusinessDataIndexCreator extends ElasticsearchIndexCreator {

  private final static Logger LOGGER = Logger.getLogger(YelpBusinessDataIndexCreator.class.getName());

  public YelpBusinessDataIndexCreator(RestHighLevelClient client,
                                      String datasetName,
                                      String datasetFilename) {
    super(client, "businesses");
  }

  @Override
  public void addMapping(Map<String, Object> mapping) {
    _mapping = new HashMap<>();

    Map<String, Object> properties = new HashMap<>();
    properties.put("business_id", getKeywordMessage());
    properties.put("name", getKeywordMessage());
    properties.put("address", getTextMessage());
    properties.put("city",getKeywordMessage());
    properties.put("state",getKeywordMessage());
    properties.put("postal_code",getKeywordMessage());
    properties.put("location", getGeoPointMessage());
    properties.put("stars", "float");
    properties.put("review_count", getIntMessage());
    properties.put("categories", getTextMessage());
    properties.put("attributes.BusinessAcceptsCreditCards", getBooleanMessage());
    properties.put("attributes.BikeParking", getBooleanMessage());
    properties.put("attributes.GoodForKids", getBooleanMessage());
    properties.put("attributes.ByAppointmentOnly", getBooleanMessage());
    properties.put("attributes.RestaurantsPriceRange2", getIntMessage());
    properties.put("attributes.BusinessParking", getNestedMessage());
    properties.put("hours", getNestedMessage());

    _mapping.put("properties", properties);
  }
}
