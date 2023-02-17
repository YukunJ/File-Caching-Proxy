/**
 * file: Server.java
 * author: Yukun Jiang
 * date: Feb 14
 *
 * This is the main file server that interact with multiple proxy
 * It implements the FileManagerRemote interface and provide
 * session semantics to its client
 * */

import static java.lang.Thread.sleep;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

public class Server extends UnicastRemoteObject implements FileManagerRemote {
  /* Server side disk manager to verify the status of a file on service dir */
  class ServerFileChecker implements FileChecker {
    public final Long SERVER_NO_EXIST = -2L;
    @Override
    public boolean IfExist(String path) {
      return new File(path).exists();
    }

    @Override
    public boolean IfDirectory(String path) {
      return new File(path).isDirectory();
    }

    @Override
    public boolean IfRegularFile(String path) {
      return new File(path).isFile();
    }

    @Override
    public boolean IfCanRead(String path) {
      return new File(path).canRead();
    }

    @Override
    public boolean IfCanWrite(String path) {
      return new File(path).canWrite();
    }

    /* server do checking for proxy, so that don't waste bandwidth transferring
       file back if the validation is failed
     */
    private int ErrorCheck(String path, FileHandling.OpenOption option) {
      boolean if_exist = IfExist(path);
      boolean if_directory = IfDirectory(path);
      boolean if_regular = IfRegularFile(path);
      boolean if_can_read = IfCanRead(path);
      boolean if_can_write = IfCanWrite(path);
      // aggregate all error cases across open mode
      if (!if_exist
          && (option != FileHandling.OpenOption.CREATE
              && option != FileHandling.OpenOption.CREATE_NEW)) {
        // the requested file not found
        return FileHandling.Errors.ENOENT;
      }
      if (if_exist && option == FileHandling.OpenOption.CREATE_NEW) {
        // cannot create new
        return FileHandling.Errors.EEXIST;
      }
      if (if_directory) {
        // the path actually points to a directory
        if (option != FileHandling.OpenOption.READ) {
          // can only apply open with read option on directory
          return FileHandling.Errors.EISDIR;
        }
        if (!if_can_read) {
          // no read permission with this directory
          return FileHandling.Errors.EPERM;
        }
        return 0;
      }
      if (!if_regular) {
        // not a regular file
        if (option == FileHandling.OpenOption.READ || option == FileHandling.OpenOption.WRITE) {
          return FileHandling.Errors.EPERM;
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          return FileHandling.Errors.EPERM;
        }
      }
      if (!if_can_read) {
        // read permission not granted
        if (option == FileHandling.OpenOption.READ) {
          return FileHandling.Errors.EPERM;
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          // not creating a new one, but old one cannot be read
          return FileHandling.Errors.EPERM;
        }
      }
      if (!if_can_write) {
        // write permission not granted
        if (option == FileHandling.OpenOption.WRITE) {
          return FileHandling.Errors.EPERM;
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          // not creating a new one, but old one cannot be written
          return FileHandling.Errors.EPERM;
        }
      }
      return 0; // no error detected
    }

    @Override
    public ValidateResult Validate(String path, FileHandling.OpenOption option, long timestamp) {
      long server_file_timestamp = file_to_timestamp_map_.getOrDefault(path, SERVER_NO_EXIST);
      int error_code = ErrorCheck(path, option);
      ValidateResult res = new ValidateResult(error_code, IfDirectory(path), server_file_timestamp);
      if (error_code == 0 && server_file_timestamp != SERVER_NO_EXIST
          && timestamp != server_file_timestamp) {
        // the server shall provide updated version to proxy
        byte[] file_data = LoadFile(path);
        res.CarryData(file_data);
      }
      return res;
    }

    /* Load a local file into an byte array, ready to be transferred over RMI */
    public byte[] LoadFile(String path) {
      assert (new File(path).exists());
      try {
        RandomAccessFile f = new RandomAccessFile(path, READER_MODE);
        byte[] data = new byte[(int) f.length()];
        f.read(data);
        return data;
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  private final ReentrantLock mtx_;
  private final HashMap<String, Long> file_to_timestamp_map_;
  private long timestamp_ = 0;
  public final String READER_MODE = "r";
  public final String WRITER_MODE = "rw";
  private static final String Slash = "/";

  private final String root_dir_;

  private final FileChecker checker_;
  public Server(String root_dir) throws RemoteException {
    super(0);
    mtx_ = new ReentrantLock();
    file_to_timestamp_map_ = new HashMap<>();
    root_dir_ = root_dir;
    checker_ = new ServerFileChecker();
    InitScanVersion();
  }

  @Override
  public ValidateResult Validate(ValidateParam param) throws RemoteException {
    String path = FormatPath(param.path);
    FileHandling.OpenOption option = param.option;
    long validation_timestamp = param.proxy_timestamp;
    Logger.Log(
        "Validate Request of path=" + path + " with proxy-side timestamp=" + validation_timestamp);
    mtx_.lock();
    try {
      return checker_.Validate(path, option, validation_timestamp);
    } finally {
      mtx_.unlock();
    }
  }

  /**
   * RMI: Upload a file to the server side, requested by proxy
   */
  @Override
  public long Upload(String path, byte[] data) throws RemoteException, IOException {
    path = FormatPath(path);
    Logger.Log("Upload Request of path=" + path + " payload=" + data.length);
    mtx_.lock();
    try {
      RandomAccessFile file = new RandomAccessFile(path, WRITER_MODE);
      // clear the content of the file if existing
      file.setLength(0);
      file.write(data);
      file.close();
      file_to_timestamp_map_.put(path, ++timestamp_);
      return timestamp_; // already incremented above
    } finally {
      mtx_.unlock();
    }
  }

  /**
   * RMI: Delete a file on the server side, requested by proxy
   */
  @Override
  public int Delete(String path) throws RemoteException {
    path = FormatPath(path);
    Logger.Log("Delete Request of path=" + path);
    mtx_.lock();
    try {
      File f = new File(path);
      if (!checker_.IfExist(path)) {
        return FileHandling.Errors.ENOENT;
      }
      if (checker_.IfDirectory(path)) {
        return FileHandling.Errors.EISDIR;
      }
      boolean success = f.delete();
      if (success) {
        file_to_timestamp_map_.remove(path);
      }
      return (success) ? 0 : FileHandling.Errors.EPERM;
    } catch (SecurityException e) {
      e.printStackTrace();
      return FileHandling.Errors.EPERM;
    } finally {
      mtx_.unlock();
    }
  }

  /**
   * Upon server starts, scan over the existing files in the service directory
   * and make initial versions for them
   */
  private void InitScanVersion() {
    // make sure the service directory exist
    assert (new File(root_dir_).isDirectory());
    mtx_.lock();
    try {
      File root = new File(root_dir_);
      ScanVersionHelper(((root.getName().equals(".")) ? "" : root.getName() + Slash), root);
      // Logging purpose
      Logger.Log("Server root_dir initial timestamp versioning:");
      for (String filename : file_to_timestamp_map_.keySet()) {
        Logger.Log(filename + " : " + file_to_timestamp_map_.get(filename));
      }
    } finally {
      mtx_.unlock();
    }
  }

  /* format a proxy-side file address to the server-side file address
   */
  private String FormatPath(String path) {
    return Paths.get((root_dir_.equals(".") ? "" : root_dir_ + Slash) + path)
        .normalize()
        .toString();
  }

  private void ScanVersionHelper(String previous_path, File directory) {
    // make sure the directory exist
    assert (directory.isDirectory());
    for (File f : directory.listFiles()) {
      if (f.isFile() && !f.isHidden()) {
        file_to_timestamp_map_.put(previous_path + f.getName(), timestamp_++);
      } else if (f.isDirectory() && !f.isHidden()) {
        ScanVersionHelper(previous_path + f.getName() + Slash, f);
      }
    }
  }

  public static void main(String[] args) throws RemoteException, MalformedURLException {
    int port = Integer.parseInt(args[0]);
    String root_dir = args[1];
    Logger.Log("Server starts running on port=" + port + " with root_dir=" + root_dir);
    try {
      LocateRegistry.createRegistry(port);
    } catch (RemoteException e) {
      e.printStackTrace();
      return;
    }

    // create our Server instance (first runs on a random port)
    Server server = new Server(root_dir);
    // add reference to registry so clients can find it using name FileServer
    String address = "//127.0.0.1:" + args[0] + Slash + FileManagerRemote.SERVER_NAME;
    Naming.rebind(address, server);
  }
}
