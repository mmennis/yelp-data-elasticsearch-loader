package yelp.data.elasticsearch.loader.data.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.*;
import java.util.Map;
import java.util.logging.Logger;

public class ElasticsearchIndexPopulator {

  private final static Logger LOGGER = Logger.getLogger(ElasticsearchIndexPopulator.class.getName());

  private File _jsonDataFile;
  private RestHighLevelClient _client;
  private GenericDataProcessor _processor;
  private String _indexName;
  private ObjectMapper _objectMapper;

  public ElasticsearchIndexPopulator(String indexName,
                                     String dataSeries,
                                     File jsonDataFile,
                                     RestHighLevelClient client) {
    this._indexName = indexName;
    this._jsonDataFile = jsonDataFile;
    this._client = client;
    this._objectMapper = new ObjectMapper();
    this._processor = DataProcessorFactory.getDataProcessor(dataSeries);
  }

  public void populateIndex() throws Exception {
    LOGGER.info("Starting index population for " + _indexName);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(this._jsonDataFile));
      String line;
      while( (line = reader.readLine()) != null) {
        Map<String, Object> jsonDataObject = processJsonLine(line);
        sendJsonDataForIndexing(jsonDataObject);
      }
    } catch (FileNotFoundException fnfe) {
      LOGGER.severe("File " + this._jsonDataFile.getAbsolutePath() + " not found");
      throw fnfe;
    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw ioe;
    } catch (Exception e) {
      LOGGER.severe("Problem parsing json string from file: " + this._jsonDataFile.getAbsolutePath());
      e.printStackTrace();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private Map<String, Object> processJsonLine(String jsonLine) throws JsonProcessingException {
    this._processor.parseJsonDataString(jsonLine);
    this._processor.process();
    return this._processor.getJsonObject();
  }

  private void sendJsonDataForIndexing(Map<String, Object> jsonData)  {
    IndexRequest request = new IndexRequest(this._indexName);
    request.id(String.valueOf(jsonData.get("id")));
    request.source(jsonData);
    try {
      IndexResponse response = this._client.index(request, RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
