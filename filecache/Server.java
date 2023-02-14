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

/* Server side disk manager to verify the status of a file on service dir */
class ServerFileChecker implements FileChecker {
  @Override
  public boolean IfExist(String path) {
    return false;
  }

  @Override
  public boolean IfDirectory(String path) {
    return false;
  }

  @Override
  public boolean IfRegularFile(String path) {
    return false;
  }

  @Override
  public boolean IfCanRead(String path) {
    return false;
  }

  @Override
  public boolean IfCanWrite(String path) {
    return false;
  }

  @Override
  public ValidateResult Validate(String path) {
    return null;
  }
}

public class Server extends UnicastRemoteObject implements FileManagerRemote {
  private final ReentrantLock mtx_;
  private final HashMap<String, Long> file_to_timestamp_map_;
  private long timestamp_ = 0;

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
  public String EchoIntBack(int i) throws RemoteException, ServerNotActiveException {
    System.out.println(getClientHost());
    return "Server echoes: " + i;
  }

  @Override
  public ValidateResult Validate(String path, long validation_version) throws RemoteException {
    return null;
  }

  /**
   * RMI: Upload a file to the server side, requested by proxy
   */
  @Override
  public long Upload(String path, byte[] data) throws RemoteException, IOException {
    path = Paths.get(root_dir_ + Slash + path).normalize().toString();
    Logger.Log("Upload Request of path=" + path + " payload=" + data.length);
    mtx_.lock();
    try {
      RandomAccessFile file = new RandomAccessFile(path, "rw");
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
    path = Paths.get(root_dir_ + Slash + path).normalize().toString();
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
    File root = new File(root_dir_);
    ScanVersionHelper(root.getName() + Slash, root);
    // Logging purpose
    Logger.Log("Server root_dir initial timestamp versioning:");
    for (String filename : file_to_timestamp_map_.keySet()) {
      Logger.Log(filename + " : " + file_to_timestamp_map_.get(filename));
    }
  }

  private void ScanVersionHelper(String previous_path, File directory) {
    // make sure the directory exist
    assert (directory.isDirectory());
    for (File f : directory.listFiles()) {
      if (f.isFile()) {
        file_to_timestamp_map_.put(previous_path + f.getName(), timestamp_++);
      } else if (f.isDirectory()) {
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
    String normalized = Paths.get("server_root/../another_dir").normalize().toString();
    System.out.println("To=" + normalized);
    System.out.println("Absolute=" + new File(normalized).getAbsolutePath());
    Server server = new Server(root_dir);

    // add reference to registry so clients can find it using name FileServer
    String address = "//127.0.0.1:" + args[0] + Slash + FileManagerRemote.SERVER_NAME;
    Naming.rebind(address, server);
  }
}
