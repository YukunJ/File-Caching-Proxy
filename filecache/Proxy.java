/**
 * file: Proxy.java
 * author: Yukun Jiang
 * date: Feb 08
 *
 * This is the main driver for the Remote File Proxy implemented
 * It interacts with the RPC Receiver to provide C-like functionality
 *
 * The real underlying work of session-semantics and cache management
 * is done in the Cache class which communicates with the Server
 */

import java.io.*;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.server.ServerNotActiveException;
import java.util.HashMap;
import java.util.HashSet;

/*
 The main driver for the Remote File Proxy
 It relies on the Cache class to take care of
 underlying file caching and replacement operation with Server
 and operates mainly on java's RandomAccessFile

 Mostly, it delegates the open/close/unlink operations to Cache
 and do the rest 3 operations on its own since it keeps a mapping
 from fd to opened RandomAccessFile
 */
class Proxy {
  /* the shared Cache for local disk, exclusive critical session */
  private static final Cache cache = new Cache();

  private static final String Slash = "/";

  private static final String Colon = ":";

  private static final int SUCCESS = 0;

  private static class FileHandler implements FileHandling {
    private static final int EIO = -5;
    private final HashMap<Integer, RandomAccessFile> fd_filehandle_map_;
    private final HashMap<Integer, OpenOption> fd_option_map_;
    private final HashSet<Integer> fd_directory_set_;
    public FileHandler() {
      fd_filehandle_map_ = new HashMap<>();
      fd_option_map_ = new HashMap<>();
      fd_directory_set_ = new HashSet<>();
    }

    /**
     * Delegate Proxy to do the real communication with Server
     * and optionally download necessary file
     */
    public int open(String path, OpenOption o) {
      String normalized_path = Paths.get(path).normalize().toString();
      // normal file, delegate to Cache
      OpenReturnVal val = cache.open(normalized_path, o);
      int fd = val.fd;
      RandomAccessFile handle = val.file_handle;
      boolean is_directory = val.is_directory;
      if (fd > SUCCESS) {
        if (!is_directory) {
          fd_filehandle_map_.put(fd, handle);
          fd_option_map_.put(fd, o);
        } else {
          fd_directory_set_.add(fd);
        }
      }
      return fd;
    }

    /**
     * When closing a file, the Proxy will upload new modification
     * to Server if any
     */
    public int close(int fd) {
      if (!fd_filehandle_map_.containsKey(fd) && !fd_directory_set_.contains(fd)) {
        return FileHandling.Errors.EBADF;
      }
      if (fd_filehandle_map_.containsKey(fd)) {
        RandomAccessFile handle = fd_filehandle_map_.get(fd);
        try {
          handle.close();
          fd_filehandle_map_.remove(fd);
          fd_option_map_.remove(fd);
          return cache.close(fd);
        } catch (IOException e) {
          return EIO;
        }
      } else {
        // close a dummy directory
        fd_directory_set_.remove(fd);
        return SUCCESS;
      }
    }

    /**
     * Write could be done locally without reaching out to Server
     * but when writing is expanding the file size, need to ask for
     * more space from the Proxy's Cache
     */
    public long write(int fd, byte[] buf) {
      if (!fd_filehandle_map_.containsKey(fd) || fd_directory_set_.contains(fd)) {
        // non-existing or write to a directory fd both give EBADF
        return FileHandling.Errors.EBADF;
      }
      if (fd_filehandle_map_.containsKey(fd) && fd_option_map_.get(fd) == OpenOption.READ) {
        // no permission to write to a read-only file
        return FileHandling.Errors.EBADF;
      }
      RandomAccessFile file_handle = fd_filehandle_map_.get(fd);
      try {
        long advance_size = file_handle.getFilePointer() + (long) buf.length - file_handle.length();
        if (advance_size > 0) {
          // exceed the current size of file, need to reserve space from cache disk
          boolean success = Cache.ReserveCacheSpace(advance_size, true);
          if (!success) {
            // exceed storage limit
            return Errors.ENOMEM;
          }
        }
        file_handle.write(buf);
        return buf.length;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return EIO;
    }

    /**
     * Read could be done without communication with Server
     */
    public long read(int fd, byte[] buf) {
      if (fd_directory_set_.contains(fd)) {
        // read from a directory fd gives EISDIR
        return Errors.EISDIR;
      }
      if (!fd_filehandle_map_.containsKey(fd)) {
        // non-existing
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

    /**
     * Lseek could be done without communcation with Server
     */
    public long lseek(int fd, long pos, LseekOption o) {
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

    /**
     * Unlink will require the Proxy to communication with the Server
     * to propogate such intent of deleting the file
     */
    public int unlink(String path) {
      String normalized_path = Paths.get(path).normalize().toString();
      return cache.unlink(normalized_path);
    }

    /*
      Nothing special, just close every open file descriptors as clean-up
     */
    public void clientdone() {
      for (int open_fd : fd_filehandle_map_.keySet()) {
        close(open_fd);
      }
    }
  }

  private static class FileHandlingFactory implements FileHandlingMaking {
    public FileHandling newclient() {
      return new FileHandler();
    }
  }

  public static void main(String[] args)
      throws IOException, NotBoundException, ServerNotActiveException {
    System.out.printf("Proxy Starts with port=%s and pin=%s\n", System.getenv("proxyport15440"),
        System.getenv("pin15440"));
    String server_address = args[0];
    String server_port = args[1];
    String cache_dir = args[2];
    Long cache_capacity = Long.parseLong(args[3]);
    String server_lookup = Slash + Slash + server_address + Colon + server_port + Slash
        + FileManagerRemote.SERVER_NAME;
    FileManagerRemote remote_manager = (FileManagerRemote) Naming.lookup(server_lookup);
    Proxy.cache.SetCacheDirectory(cache_dir);
    Proxy.cache.SetCacheCapacity(cache_capacity);
    Proxy.cache.AddRemoteFileManager(remote_manager);
    (new RPCreceiver(new FileHandlingFactory())).run();
  }
}
