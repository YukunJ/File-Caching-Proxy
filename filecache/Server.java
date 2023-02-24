/**
 * file: Server.java
 * author: Yukun Jiang
 * date: Feb 14
 *
 * This is the main file server that interact with multiple proxy
 * It implements the FileManagerRemote interface and provide
 * session semantics to its client
 * */
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/* Remote File Server meeting session-semantics and check-on-use cache
 * consistent policy */
public class Server extends UnicastRemoteObject implements FileManagerRemote {
  public enum LOCK_MODE {
    READ,
    WRITE;
  }

  /* Server side disk manager to verify the status of a file on service dir */
  class ServerFileChecker implements FileChecker {
    private final int SUCCESS = 0;
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
      /*
       * Helper Proxy make decisions of if such a request should succeed
       * so that could save traffic of transferring a file if request fails
       */
      boolean if_exist = IfExist(path);
      boolean if_directory = IfDirectory(path);
      boolean if_regular = IfRegularFile(path);
      boolean if_can_read = IfCanRead(path);
      boolean if_can_write = IfCanWrite(path);
      // aggregate all error cases across open mode
      if (!if_exist && (option != FileHandling.OpenOption.CREATE &&
                        option != FileHandling.OpenOption.CREATE_NEW)) {
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
        return SUCCESS;
      }
      if (!if_regular) {
        // not a regular file
        if (option == FileHandling.OpenOption.READ ||
            option == FileHandling.OpenOption.WRITE) {
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
      return SUCCESS; // no error detected
    }

    /**
     * Validate if Proxy's open request for a file should succeed
     * if needed, transfer newest version of the file to Proxy
     */
    @Override
    public ValidateResult Validate(String path, FileHandling.OpenOption option,
                                   long timestamp) {
      GrabLock(path, LOCK_MODE.READ); // lock in reader mode
      long server_file_timestamp =
          file_to_timestamp_map_.getOrDefault(path, SERVER_NO_EXIST);
      int error_code = ErrorCheck(path, option);
      ValidateResult res = new ValidateResult(error_code, IfDirectory(path),
                                              server_file_timestamp);
      if (error_code == SUCCESS && server_file_timestamp != SERVER_NO_EXIST &&
          timestamp != server_file_timestamp) {
        // the server shall provide updated version to proxy
        FileChunk chunk = LoadFile(path);
        res.CarryChunk(chunk);
      } else {
        ReleaseLock(path, LOCK_MODE.READ);
      }
      return res;
    }

    /* Load a local file to be sent in chunk-by-chunk fashion */
    public FileChunk LoadFile(String path) {
      try {
        Integer chunk_id = file_chunk_id++;
        RandomAccessFile f = new RandomAccessFile(path, READER_MODE);
        Integer whole_file_size = (int)f.length();
        Integer max_chunk_size = FileChunk.CHUNK_SIZE;
        Integer chunk_size = Math.min(whole_file_size, max_chunk_size);
        byte[] data = new byte[chunk_size];
        f.read(data);
        boolean is_end = (max_chunk_size >= whole_file_size);
        if (!is_end) {
          file_download_chunk_map_.put(chunk_id, f);
          chunk_id_to_file_.put(chunk_id, path);
        } else {
          f.close();
          ReleaseLock(path, LOCK_MODE.READ);
        }
        return new FileChunk(data, is_end, chunk_id);
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }
  }

  /*
    grab the lock for a specific file in either reader/writer mode
    any modification on the file_to_lock_ map needs global synchronization
   */
  private void GrabLock(String path, LOCK_MODE mode) {
    if (!file_to_lock_.containsKey(path)) {
      mtx_.lock();
      try {
        if (!file_to_lock_.containsKey(path)) {
          file_to_lock_.put(path, new ReentrantReadWriteLock());
        }
      } finally {
        mtx_.unlock();
      }
    }
    if (mode == LOCK_MODE.READ) {
      file_to_lock_.get(path).readLock().lock();
    } else {
      file_to_lock_.get(path).writeLock().lock();
    }
  }

  /*
    grab the lock for a specific file in either reader/writer mode
    any modification on the file_to_lock_ map needs global synchronization
   */
  private void ReleaseLock(String path, LOCK_MODE mode) {
    if (mode == LOCK_MODE.READ) {
      file_to_lock_.get(path).readLock().unlock();
    } else {
      file_to_lock_.get(path).writeLock().unlock();
    }
  }

  public static final Long SERVER_NO_EXIST = -2L;

  private final ReentrantLock mtx_;

  private final HashMap<String, ReadWriteLock> file_to_lock_;

  private final HashMap<Integer, String> chunk_id_to_file_;

  private final HashMap<String, Long> file_to_timestamp_map_;
  private Integer file_chunk_id = 0;
  private final HashMap<Integer, RandomAccessFile> file_download_chunk_map_;

  private final HashMap<Integer, RandomAccessFile> file_upload_chunk_map_;
  private long timestamp_ = 0;
  public final String READER_MODE = "r";
  public final String WRITER_MODE = "rw";
  private static final String Slash = "/";

  private static final String BACKWARD = "..";

  private final String root_dir_;

  private final static int SUCCESS = 0;
  private static final int TIMESTAMP_INDEX = 0;

  private static final int CHUNK_INDEX = 1;

  private static final int ZERO = 0;

  private static final int TUPLE_SIZE = 2;

  private final FileChecker checker_;
  public Server(String root_dir) throws RemoteException {
    super(0);
    mtx_ = new ReentrantLock();
    file_to_lock_ = new HashMap<>();
    chunk_id_to_file_ = new HashMap<>();
    file_to_timestamp_map_ = new HashMap<>();
    file_download_chunk_map_ = new HashMap<>();
    file_upload_chunk_map_ = new HashMap<>();
    root_dir_ = root_dir;
    checker_ = new ServerFileChecker();
    InitScanVersion();
  }

  /*
    Delegate to file_checker to do the real checking mechanism rountine
   */
  @Override
  public ValidateResult Validate(ValidateParam param) throws RemoteException {
    String path = FormatPath(param.path);
    if (path.startsWith(BACKWARD)) {
      // access out of root directory
      return new ValidateResult(FileHandling.Errors.EPERM,
                                checker_.IfDirectory(path), SERVER_NO_EXIST);
    }
    FileHandling.OpenOption option = param.option;
    long validation_timestamp = param.proxy_timestamp;
    return checker_.Validate(path, option, validation_timestamp);
  }

  /*
     When file is too big, Proxy will continually download file chunk by chunk
     while doing so, it needs to hold a reader lock for this file
     to make sure no one uploads a new version in the middle of downloading
   */
  @Override
  public FileChunk DownloadChunk(Integer chunk_id)
      throws IOException, RemoteException {
    RandomAccessFile f = file_download_chunk_map_.get(chunk_id);
    long curr_pos = f.getFilePointer();
    long total_len = f.length();
    Integer file_remain_length = (int)(total_len - curr_pos);
    Integer max_chunk_size = FileChunk.CHUNK_SIZE;
    Integer chunk_size = Math.min(file_remain_length, max_chunk_size);
    byte[] data = new byte[chunk_size];
    f.read(data);
    boolean is_end = (max_chunk_size >= file_remain_length);
    if (is_end) {
      String full_path = chunk_id_to_file_.get(chunk_id);
      file_download_chunk_map_.remove(chunk_id);
      chunk_id_to_file_.remove(chunk_id);
      ReleaseLock(full_path, LOCK_MODE.READ);
    }
    return new FileChunk(data, is_end, chunk_id);
  }

  /**
   * RMI: Upload a file to the server side, requested by proxy
   * may subsequentlly call upload_chunk if too big a file
   */
  @Override
  public Long[] Upload(String path, FileChunk chunk)
      throws RemoteException, IOException {
    path = FormatPath(path);
    GrabLock(path, LOCK_MODE.WRITE);
    Long chunk_id = (long)file_chunk_id++;
    RandomAccessFile file = new RandomAccessFile(path, WRITER_MODE);
    // clear the content of the file if existing
    file.setLength(ZERO);
    file.write(chunk.data);
    if (chunk.end_of_file) {
      file.close();
      ReleaseLock(path, LOCK_MODE.WRITE);
    } else {
      file_upload_chunk_map_.put(chunk_id.intValue(), file);
      chunk_id_to_file_.put(chunk_id.intValue(), path);
    }
    file_to_timestamp_map_.put(path, ++timestamp_);
    Long[] tuple = new Long[TUPLE_SIZE];
    tuple[TIMESTAMP_INDEX] = timestamp_;
    tuple[CHUNK_INDEX] = chunk_id;
    return tuple;
  }

  /**
   * Upload the specific chunk of a large file from Proxy to Server
   */
  @Override
  public void UploadChunk(FileChunk chunk) throws RemoteException, IOException {
    String full_path = chunk_id_to_file_.get(chunk.chunk_id);
    RandomAccessFile f = file_upload_chunk_map_.get(chunk.chunk_id);
    f.write(chunk.data);
    if (chunk.end_of_file) {
      file_upload_chunk_map_.remove(chunk.chunk_id);
      chunk_id_to_file_.remove(chunk.chunk_id);
      ReleaseLock(full_path, LOCK_MODE.WRITE); // writer unlock
    }
  }

  /*
    When the proxy doesn't have enough space, send the cancel chunk request to
    actively unlock must be a reader lock, writer upload always succeed in terms
    of storage space
   */
  @Override
  public void CancelChunk(Integer chunk_id) throws RemoteException {
    String full_path = chunk_id_to_file_.get(chunk_id);
    chunk_id_to_file_.remove(chunk_id);
    ReleaseLock(full_path, LOCK_MODE.READ);
  }

  /**
   * RMI: Delete a file on the server side, requested by proxy
   */
  @Override
  public int Delete(String path) throws RemoteException {
    path = FormatPath(path);
    GrabLock(path, LOCK_MODE.WRITE);
    try {
      File f = new File(path);
      if (!checker_.IfExist(path)) {
        ReleaseLock(path, LOCK_MODE.WRITE);
        return FileHandling.Errors.ENOENT;
      }
      if (checker_.IfDirectory(path)) {
        ReleaseLock(path, LOCK_MODE.WRITE);
        return FileHandling.Errors.EISDIR;
      }
      boolean success = f.delete();
      if (success) {
        file_to_timestamp_map_.remove(path);
      }
      ReleaseLock(path, LOCK_MODE.WRITE);
      return (success) ? SUCCESS : FileHandling.Errors.EPERM;
    } catch (SecurityException e) {
      e.printStackTrace();
      return FileHandling.Errors.EPERM;
    }
  }

  /**
   * Upon server starts, scan over the existing files in the service directory
   * and make initial versions for them
   */
  private void InitScanVersion() {
    // make sure the service directory exist
    mtx_.lock();
    try {
      File root = new File(root_dir_);
      ScanVersionHelper(
          ((root.getName().equals(".")) ? "" : root.getName() + Slash), root);
    } finally {
      mtx_.unlock();
    }
  }

  /*
   * format a proxy-side file address to the server-side file address
   */
  private String FormatPath(String path) {
    return Paths.get((root_dir_.equals(".") ? "" : root_dir_ + Slash) + path)
        .normalize()
        .toString();
  }

  /*
    Recursive call to scanning the server's initial root directory
   */
  private void ScanVersionHelper(String previous_path, File directory) {
    // make sure the directory exist
    for (File f : directory.listFiles()) {
      if (f.isFile() && !f.isHidden()) {
        String full_path = previous_path + f.getName();
        file_to_timestamp_map_.put(full_path, timestamp_++);
        file_to_lock_.put(full_path, new ReentrantReadWriteLock());
      } else if (f.isDirectory() && !f.isHidden()) {
        ScanVersionHelper(previous_path + f.getName() + Slash, f);
      }
    }
  }

  public static void main(String[] args)
      throws RemoteException, MalformedURLException {
    int port = Integer.parseInt(args[0]);
    String root_dir = args[1];
    try {
      LocateRegistry.createRegistry(port);
    } catch (RemoteException e) {
      e.printStackTrace();
      return;
    }

    // create our Server instance (first runs on a random port)
    Server server = new Server(root_dir);
    // add reference to registry so clients can find it using name FileServer
    String address =
        "//127.0.0.1:" + args[0] + Slash + FileManagerRemote.SERVER_NAME;
    Naming.rebind(address, server);
  }
}
