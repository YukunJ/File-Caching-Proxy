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

/*
 The main driver for the Remote File Proxy
 It relies on the Cache class to take care of
 underlying file caching and replacement operation
 and operates mainly on java's RandomAccessFile
 */
class Proxy {
  private static class FileHandler implements FileHandling {
    public int open(String path, OpenOption o) {
      System.out.println("Get a open request lol");
      return Errors.ENOSYS;
    }

    public int close(int fd) {
      return Errors.ENOSYS;
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
