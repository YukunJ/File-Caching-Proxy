/**
 * file: Proxy.java
 * author: Yukun Jiang
 * date: Feb 08
 *
 * This is the main driver for the Remote File Proxy implemented
 * It interacts with the RPC Receiver to provide C-like functionality
 *
 * The real underlying work of session-semantics and cache management
 * is done in the Cache class
 */

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;

/*
 The main driver for the Remote File Proxy
 It relies on the Cache class to take care of
 underlying file caching and replacement operation
 and operates mainly on java's RandomAccessFile
 */
class Proxy {
  private static final Cache cache = new Cache();
  private static class FileHandler implements FileHandling {
    private final static int DIRECTORY_FD_OFFSET = 2048;
    private int directory_fd_;
    private final HashMap<Integer, RandomAccessFile> fd_filehandle_map_;
    private final HashSet<Integer> fd_directory_set_;
    public FileHandler() {
      fd_filehandle_map_ = new HashMap<>();
      fd_directory_set_ = new HashSet<>();
      directory_fd_ = DIRECTORY_FD_OFFSET;
    }

    public int open(String path, OpenOption o) {
      String normalized_path = Paths.get(path).normalize().toString();
      Logger.Log("open request: path=" + path + " normalized=" + normalized_path
          + " option=" + Logger.OpenOptionToString(o));
      // TODO: check if this path is within cache directory
      if (new File(normalized_path).isDirectory()) {
        // handle all the directory operations here
        if (o != OpenOption.READ) {
          // directory could only be read
          return Errors.EISDIR;
        }
        if (new File(normalized_path).canRead()) {
          // not going to do anything with the directory fd, give back a dummy
          int fd = directory_fd_++;
          fd_directory_set_.add(fd);
          return fd;
        } else {
          return Errors.EPERM;
        }
      }
      // normal file, delegate to Cache
      OpenReturnVal val = cache.open(normalized_path, o);
      int fd = val.fd_;
      RandomAccessFile handle = val.file_handle_;
      if (fd > 0) {
        fd_filehandle_map_.put(fd, handle);
      }
      return fd;
    }

    public int close(int fd) {
      Logger.Log("close request: fd=" + fd);
      if (!fd_filehandle_map_.containsKey(fd) && !fd_directory_set_.contains(fd)) {
        return FileHandling.Errors.EBADF;
      }
      if (fd_filehandle_map_.containsKey(fd)) {
        fd_filehandle_map_.remove(fd);
        return cache.close(fd);
      } else {
        // close a dummy directory
        fd_directory_set_.remove(fd);
        return 0;
      }
    }

    public long write(int fd, byte[] buf) {
      return Errors.ENOSYS;
    }

    public long read(int fd, byte[] buf) {
      return Errors.ENOSYS;
    }

    public long lseek(int fd, long pos, LseekOption o) {
      return Errors.ENOSYS;
    }

    public int unlink(String path) {
      return Errors.ENOSYS;
    }

    public void clientdone() {
      return;
    }
  }

  private static class FileHandlingFactory implements FileHandlingMaking {
    public FileHandling newclient() {
      return new FileHandler();
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.printf("Proxy Starts with port=%s and pin=%s\n", System.getenv("proxyport15440"),
        System.getenv("pin15440"));
    (new RPCreceiver(new FileHandlingFactory())).run();
  }
}
