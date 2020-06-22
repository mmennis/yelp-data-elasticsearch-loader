package yelp.data.elasticsearch.loader;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Properties;

public class ElasticsearchRestClientFactory {

  public static RestHighLevelClient getClient(Properties props){
    String host = props.getProperty("elasticsearch.host", "localhost");
    String portNo = props.getProperty("elasticsearch.port", "9200");
    return getClient(host, Integer.parseInt(portNo));
  }

  private static RestHighLevelClient getClient(String host, int port) {
    return new RestHighLevelClient(
        RestClient.builder(new HttpHost(host, port)));
  }

  private static RestHighLevelClient getSecureClient(String host,
                                              int port,
                                              String username,
                                              String password) {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(username, password));

    RestClientBuilder builder = RestClient.builder(
        new HttpHost(host, port, "http")).setHttpClientConfigCallback(
        httpClientBuilder ->
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
    );
    return new RestHighLevelClient(builder);
  }
}
