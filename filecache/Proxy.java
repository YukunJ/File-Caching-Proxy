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

 Mostly, it delegates the open/close operations to Cache
 and do the rest 4 operations on its own since it keeps a mapping
 from fd to opened RandomAccessFile
 */
class Proxy {
  private static final Cache cache = new Cache();
  private static class FileHandler implements FileHandling {
    /* dummy offset for directory-only */
    private final static int DIRECTORY_FD_OFFSET = 2048;
    private static final int EIO = -5;
    private int directory_fd_;
    private final HashMap<Integer, RandomAccessFile> fd_filehandle_map_;
    private final HashMap<Integer, OpenOption> fd_option_map_;
    private final HashSet<Integer> fd_directory_set_;
    public FileHandler() {
      fd_filehandle_map_ = new HashMap<>();
      fd_option_map_ = new HashMap<>();
      fd_directory_set_ = new HashSet<>();
      directory_fd_ = DIRECTORY_FD_OFFSET;
    }

    public int open(String path, OpenOption o) {
      String normalized_path = Paths.get(path).normalize().toString();
      // TODO: check if this path is within cache directory
      if (new File(normalized_path).isDirectory()) {
        // handle all the directory operations here
        if (o != OpenOption.READ) {
          // directory could only be read
          return Errors.EISDIR;
        }
        if (new File(normalized_path).canRead()) {
          // not going to do anything with the directory fd, give back a dummy fd
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
        fd_option_map_.put(fd, o);
      }
      Logger.Log("open request: path=" + path + " normalized=" + normalized_path
          + " option=" + Logger.OpenOptionToString(o) + " with return_fd=" + fd);
      return fd;
    }

    public int close(int fd) {
      Logger.Log("close request: fd=" + fd);
      if (!fd_filehandle_map_.containsKey(fd) && !fd_directory_set_.contains(fd)) {
        return FileHandling.Errors.EBADF;
      }
      if (fd_filehandle_map_.containsKey(fd)) {
        fd_filehandle_map_.remove(fd);
        fd_option_map_.remove(fd);
        return cache.close(fd);
      } else {
        // close a dummy directory
        fd_directory_set_.remove(fd);
        return 0;
      }
    }

    public long write(int fd, byte[] buf) {
      Logger.Log("write request: fd=" + fd + " of size " + buf.length);
      if (!fd_filehandle_map_.containsKey(fd) || fd_directory_set_.contains(fd)) {
        // non-existing write to a directory fd gives EBADF
        return FileHandling.Errors.EBADF;
      }
      if (fd_filehandle_map_.containsKey(fd) && fd_option_map_.get(fd) == OpenOption.READ) {
        // no permission to write to a read-only file
        return FileHandling.Errors.EBADF;
      }
      RandomAccessFile file_handle = fd_filehandle_map_.get(fd);
      try {
        file_handle.write(buf);
        return buf.length;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return EIO;
    }

    public long read(int fd, byte[] buf) {
      Logger.Log("read request: fd=" + fd + " with destination capacity=" + buf.length);
      if (fd_directory_set_.contains(fd)) {
        // read from a directory fd gives EISDIR
        return Errors.EISDIR;
      }
      if (!fd_filehandle_map_.containsKey(fd) || fd_option_map_.get(fd) == OpenOption.WRITE) {
        // non-existing or write-permission only
        return Errors.EBADF;
      }
      RandomAccessFile file_handle = fd_filehandle_map_.get(fd);
      try {
        int byte_reads = file_handle.read(buf);
        if (byte_reads == -1) {
          // EOF encountered
          return 0;
        }
        return byte_reads;
      } catch (Exception e) {
        e.printStackTrace();
      }
      // unknown exception placeholder
      return EIO;
    }

    public long lseek(int fd, long pos, LseekOption o) {
      Logger.Log("lseek request: fd=" + fd + " pos=" + pos
          + " LseekOption=" + Logger.SeekOptionToString(o));
      if (!fd_filehandle_map_.containsKey(fd) || fd_directory_set_.contains(fd)) {
        return Errors.EBADF;
      }
      RandomAccessFile file_handle = fd_filehandle_map_.get(fd);
      try {
        long curr_pos = file_handle.getFilePointer();
        long total_len = file_handle.length();
        long target_pos;
        if (o == LseekOption.FROM_CURRENT) {
          target_pos = curr_pos + pos;
        } else if (o == LseekOption.FROM_START) {
          target_pos = pos;
        } else {
          target_pos = total_len + pos;
        }
        if (target_pos < 0) {
          // negative seek is not allowed
          return Errors.EINVAL;
        }
        file_handle.seek(target_pos);
        return target_pos;
      } catch (IOException e) {
        e.printStackTrace();
      }
      // unknown exception placeholder
      return EIO;
    }

    public int unlink(String path) {
      String normalized_path = Paths.get(path).normalize().toString();
      Logger.Log("unlink request: path=" + path + " normalized=" + normalized_path);
      return cache.unlink(normalized_path);
    }

    public void clientdone() {
      // TODO: in cpkt2&3, notice proxy/server to clear up space
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
