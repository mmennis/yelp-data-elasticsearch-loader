package yelp.data.elasticsearch.loader;

import java.io.File;

public class YelpDataLoader {

  private File _datasetDirectory;

  public YelpDataLoader(File datasetDirectory) {
    this._datasetDirectory = datasetDirectory;
  }

  public void execute() throws Exception {
    if ( getDatasetDirectory() == null ) {
      throw new Exception("Missing directory location for yelp datasets");
    }
  }

  public File getDatasetDirectory() {
    return _datasetDirectory;
  }

  public void setDatasetDirectory(File datasetDirectory) {
    this._datasetDirectory = datasetDirectory;
  }
}
