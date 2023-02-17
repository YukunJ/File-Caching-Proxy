/**
 * file: ValidateParam.java
 * author: Yukun Jiang
 * date: Feb 16
 *
 * This is input parameter class from proxy to server
 * about the version validation of a file open request
 *
 * if the proxy version is stale, server will supply the fresh
 * file in this return value, along with existence/directory/read/write permission flags, etc
 * */

import java.io.Serializable;

/* the aggregated input info when check-on-use with server */
public class ValidateParam implements Serializable {
  String path;

  FileHandling.OpenOption option;

  long proxy_timestamp;

  public ValidateParam(String path, FileHandling.OpenOption option, long proxy_timestamp) {
    this.path = path;
    this.option = option;
    this.proxy_timestamp = proxy_timestamp;
  }
}