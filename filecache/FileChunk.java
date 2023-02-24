/**
 * file: ValidateResult.java
 * author: Yukun Jiang
 * date: Feb 21
 *
 * This is the file chunking abstraction. To avoid a large memory overhead
 * and too big a RPC transfer, we chunk large function into smaller chunks
 * */

import java.io.Serializable;

/**
 * File Chunk used when uploading/downloading a large file from Server
 * to avoid large RPC call messages on the fly
 */
public class FileChunk implements Serializable {
  /* 200KB tunable chunk size by default */
  public static Integer CHUNK_SIZE = 200 * 1024;

  byte[] data = null;
  boolean end_of_file;

  Integer chunk_id;
  FileChunk(byte[] data, boolean end_of_file, int chunk_id) {
    this.data = data;
    this.end_of_file = end_of_file;
    this.chunk_id = chunk_id;
  }

  void SetData(byte[] data) { this.data = data; }

  void SetEndOfFile(boolean end_of_file) { this.end_of_file = end_of_file; }

  void SetChunkId(Integer id) { this.chunk_id = id; }
}
