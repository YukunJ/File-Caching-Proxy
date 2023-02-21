/**
 * file: ValidateResult.java
 * author: Yukun Jiang
 * date: Feb 15
 *
 * This is return value class from server back to proxy
 * about the version validation of a file open request
 *
 * if the proxy version is stale, server will supply the fresh
 * file in this return value, along with existence/directory/read/write permission flags, etc
 * */

import java.io.Serializable;

/* the aggregated return info when check-on-use with server */
public class ValidateResult implements Serializable {
  int error_code;

  boolean is_directory;
  long timestamp;

  FileChunk chunk;

  public ValidateResult(int error_code, boolean is_directory, long timestamp) {
    this.error_code = error_code;
    this.is_directory = is_directory;
    this.timestamp = timestamp;
    this.chunk = null;
  }

  public void CarryChunk(FileChunk file_chunk) {
    this.chunk = file_chunk;
  }
}