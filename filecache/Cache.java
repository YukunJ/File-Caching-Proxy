/**
 * file: Cache.java
 * author: Yukun Jiang
 * date: Feb 08
 *
 * This is the underlying cache management
 * It needs to control the overall size of cached file
 *
 * and to retain session semantics, it will return a
 * RandomAccessFile to Proxy upon request, and depending whether
 * it's read mode or write mode, may create a new temp file or use old file
 *
 * File are represented as FileRecord, which records the version number of one file
 * and keep reference count of each version and delete the version when refernce
 * count drops to zero.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/* For Cache returns to Proxy */
class OpenReturnVal {
  /* if success, a RandomAccessFile handle is returned */
  public RandomAccessFile file_handle;
  /* might be negative to indicate error */
  public int fd;
  /* might be a trivial directoyr */
  public boolean is_directory;
  OpenReturnVal(RandomAccessFile file_handle, int fd, boolean is_directory) {
    this.file_handle = file_handle;
    this.fd = fd;
    this.is_directory = is_directory;
  }
}

/* For FileRecord returns to Cache */
class FileReturnVal {
  public RandomAccessFile file_handle_;
  public int version_;
  public FileReturnVal(RandomAccessFile handle, int version) {
    file_handle_ = handle;
    version_ = version;
  }
}
/**
 * The version info about a file
 * one file might have different versions at the same time due to concurrency
 */
class Version {
  public final int version_;
  public final String filename_;
  private int ref_count_;

  public Version(String filename, int version) {
    filename_ = filename;
    version_ = version;
    ref_count_ = 0;
  }

  public int GetRefCount() {
    return ref_count_;
  }

  public int PlusRefCount() {
    return ++ref_count_;
  }

  public int MinusRefCount() {
    return --ref_count_;
  }

  public String ToFileName() {
    return filename_ + ((version_ == 0) ? "" : Integer.toString(version_));
  }
}

class FileRecord {
  private final String filename_;

  public final HashMap<Integer, Version> version_map_;

  /* the latest reader-visible version, if -1 means no one can see this file now */
  private int reader_version_;

  /* the latest spawn version by writer, monotonically increasing */
  private int latest_version_;

  /* if the file already exists on disk, reader_version is init to 0
     if new writer create this file, reader_version is init to -1 so no one sees it until commit
   */
  public FileRecord(String filename, int reader_version, int latest_version) {
    filename_ = filename;
    reader_version_ = reader_version;
    latest_version_ = latest_version;
    version_map_ = new HashMap<>();
    if (reader_version == 0) {
      version_map_.put(0, new Version(filename, 0));
    }
  }

  /**
   * Get a shared copy of the reader version of this file
   * caller should ensure that reader_version >= 0 already
   */
  public FileReturnVal GetReaderFile() throws Exception {
    assert (reader_version_ >= 0);
    int reader_version_id = GetReaderVersionId();
    Version reader_version = GetReaderVersion();
    String reader_filename = reader_version.ToFileName();
    String cache_reader_filepath = Cache.FormatPath(reader_filename);
    RandomAccessFile file_handle = new RandomAccessFile(cache_reader_filepath, Cache.READER_MODE);
    Logger.Log("GerReaderFile for cache_path=" + cache_reader_filepath
        + " with reader_version=" + reader_version_id);
    reader_version.PlusRefCount();
    return new FileReturnVal(file_handle, reader_version_id);
  }

  /**
   * Get an exclusive writer copy of this file
   * if an existing version exist, make a copy and start from there
   * otherwise create an empty file to start work with
   */
  public FileReturnVal GetWriterFile() throws Exception {
    int writer_version_id = IncrementLatestVersionId();
    Version writer_version = new Version(filename_, writer_version_id);
    String writer_filename = writer_version.ToFileName();
    String cache_writer_filepath = Cache.FormatPath(writer_filename);
    Logger.Log("GetWriteFile() for cache_path=" + cache_writer_filepath
        + " with writer_version=" + writer_version.version_);
    if (GetReaderVersionId() >= 0) {
      // there is existing version, copy it
      String reader_filename = GetReaderVersion().ToFileName();
      String cache_reader_filepath = Cache.FormatPath(reader_filename);
      CopyFile(cache_writer_filepath, cache_reader_filepath);
      Logger.Log(
          "Make a cache copy from " + cache_reader_filepath + " to " + cache_writer_filepath);
    }
    RandomAccessFile file_handle = new RandomAccessFile(cache_writer_filepath, Cache.WRITER_MODE);
    version_map_.put(writer_version_id, writer_version); // must be an exclusive version
    writer_version.PlusRefCount();
    return new FileReturnVal(file_handle, writer_version_id);
  }

  /**
   * Close a shared reader version of this file
   * and additionally clean up if no one else will see/use this version
   */
  public void CloseReaderFile(int version_id) {
    Version reader_version = version_map_.get(version_id);
    int remain_ref_count = reader_version.MinusRefCount();
    if (remain_ref_count == 0 && version_id != GetReaderVersionId()) {
      // no more client will see this version
      // version_map_.remove(version_id);
      // TODO: remove this version's associated disk file to clear cache space
    }
  }

  /**
   * Close an exclusive version of this file
   * and install this to be the newest visible reader version
   */
  public void CloseWriterFile(int version_id) {
    Version writer_version = version_map_.get(version_id);
    byte[] data;
    try {
      RandomAccessFile file =
          new RandomAccessFile(Cache.FormatPath(writer_version.ToFileName()), Cache.READER_MODE);
      data = new byte[(int) file.length()];
      file.read(data);
      file.close();

      // must be 0 now
      assert (writer_version.MinusRefCount() == 0);
      // install to be available new reader version
      int install_version_id = writer_version.version_;
      SetReaderVersionId(install_version_id);
      // should not remove this version from map, future reader need it

      // upload new version to server and record the timestamp
      String origin_filename = writer_version.filename_;
      Long server_timestamp = Cache.remote_manager_.Upload(origin_filename, data);
      Cache.UpdateTimestamp(origin_filename, server_timestamp);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int IncrementLatestVersionId() {
    return ++latest_version_;
  }
  public int GetLatestVersionId() {
    return latest_version_;
  }

  public int GetReaderVersionId() {
    return reader_version_;
  }

  public Version GetReaderVersion() {
    return version_map_.get(reader_version_);
  }
  public void SetReaderVersionId(int new_reader_version) {
    int old_reader_version_ = reader_version_;
    if (old_reader_version_ != new_reader_version && GetReaderVersion() != null
        && GetReaderVersion().GetRefCount() == 0) {
      // #TODO: no one refers to old reader version, could do some clean up
    }
    reader_version_ = new_reader_version;
  }

  /**
   * copy the file specified by src_name into a new file named by dest_name
   */
  public static void CopyFile(String dest_name, String src_name) throws Exception {
    File src = new File(src_name);
    File dest = new File(dest_name);
    Files.copy(src.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * delete a file specified by the name
   */
  public static void DeleteFile(String name) {
    File file = new File(name);
    try {
      boolean success = file.delete();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

public class Cache {
  /* file descriptor offset */
  private static final int INIT_FD = 1024;
  private static final int EIO = -5;
  private static final Long CACHE_NO_EXIST = -1L;
  public static final String READER_MODE = "r";
  public static final String WRITER_MODE = "rw";
  private static final String Slash = "/";
  public static FileManagerRemote remote_manager_; // to communicate with Server
  private static final HashMap<String, Long> timestamp_map_ = new HashMap<>();
  private int cache_fd_;
  private final HashMap<String, FileRecord> record_map_;
  private final HashMap<Integer, RandomAccessFile> fd_handle_map_;
  private final HashMap<Integer, String> fd_filename_map_;

  private final HashMap<Integer, Integer> fd_version_map_;

  private final HashMap<Integer, FileHandling.OpenOption> fd_option_map_;

  private final ReentrantLock mtx_;

  private static String cache_dir_;

  public Cache() {
    cache_fd_ = INIT_FD;
    record_map_ = new HashMap<>();
    fd_handle_map_ = new HashMap<>();
    fd_filename_map_ = new HashMap<>();
    fd_version_map_ = new HashMap<>();
    fd_option_map_ = new HashMap<>();
    mtx_ = new ReentrantLock();
  }

  public static void UpdateTimestamp(String path, Long timestamp) {
    timestamp_map_.put(path, timestamp);
  }

  /* set the cache root directory for disk storage */
  public void SetCacheDirectory(String cache_dir) {
    cache_dir_ = cache_dir;
  }

  /* add the handler to enable communicating with Server */
  public void AddRemoteFileManager(FileManagerRemote remote_manager) {
    this.remote_manager_ = remote_manager;
  }

  /* do book-keeping after a successful open */
  public int Register(
      String filename, RandomAccessFile file_handle, int version, FileHandling.OpenOption option) {
    // generate a fd for this register request
    int fd = cache_fd_++;
    // register book-keeping
    fd_handle_map_.put(fd, file_handle);
    fd_filename_map_.put(fd, filename);
    fd_option_map_.put(fd, option);
    fd_version_map_.put(fd, version);
    return fd;
  }

  /* after making sure we can access and open this file, make such a request */
  public OpenReturnVal GetAndRegisterFile(
      FileRecord record, String path, FileHandling.OpenOption option) throws Exception {
    FileReturnVal return_val;
    if (option == FileHandling.OpenOption.READ) {
      return_val = record.GetReaderFile();
    } else {
      return_val = record.GetWriterFile();
    }
    RandomAccessFile file_handle = return_val.file_handle_;
    int version = return_val.version_;
    // register for bookkeeping
    int fd = Register(path, file_handle, version, option);
    return new OpenReturnVal(file_handle, fd, false);
  }

  /* Upon closing a fd, remove it from mapping record */
  public void DeregisterFile(int fd) throws Exception {
    RandomAccessFile file_handle = fd_handle_map_.get(fd);
    int version_id = fd_version_map_.get(fd);
    FileHandling.OpenOption option = fd_option_map_.get(fd);
    String filename = fd_filename_map_.get(fd);
    FileRecord record = record_map_.get(filename);
    file_handle.close();
    // need to physically close this file
    // before make it visible to other threads
    if (option == FileHandling.OpenOption.READ) {
      record.CloseReaderFile(version_id);
    } else {
      record.CloseWriterFile(version_id);
    }
    fd_handle_map_.remove(fd);
    fd_version_map_.remove(fd);
    fd_option_map_.remove(fd);
    fd_filename_map_.remove(fd);
  }

  /* map the logical file path to the cache root directory file path */
  public static String FormatPath(String path) {
    return Paths.get(cache_dir_ + Slash + path).normalize().toString();
  }

  /* save a file transferred from server into local cache directory */
  private void SaveData(String path, byte[] data, Long server_timestamp) {
    try {
      FileRecord record = record_map_.get(path);
      if (record == null) {
        record = new FileRecord(path, -1, -1);
        record_map_.put(path, record);
      }
      int version_id = record.IncrementLatestVersionId();
      Version version = new Version(path, version_id);
      record.version_map_.put(version_id, version);

      String cache_path = FormatPath(version.ToFileName());
      assert (cache_path.startsWith(cache_dir_) && data != null);
      Logger.Log(
          "SaveData for cache_path=" + cache_path + " of size=" + data.length + " from server");
      RandomAccessFile file = new RandomAccessFile(cache_path, WRITER_MODE);
      file.setLength(0); // clear off content
      file.write(data);
      file.close();
      UpdateTimestamp(path, server_timestamp); // save the server timestamp for original path

      record.SetReaderVersionId(version_id); // make this version available to clients
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Proxy delegate the open functionality to cache
   * and cache make local disk operations based on check-on-use results from the server
   * it should properly handle Exception and return corresponding error code if applicable
   */
  public OpenReturnVal open(String path, FileHandling.OpenOption option) {
    mtx_.lock();
    try {
      long cache_file_timestamp = timestamp_map_.getOrDefault(path, CACHE_NO_EXIST);
      /* send validation request to server */
      ValidateResult validate_result =
          remote_manager_.Validate(new ValidateParam(path, option, cache_file_timestamp));
      int error_code = validate_result.error_code;
      boolean if_directory = validate_result.is_directory;
      if (error_code == FileHandling.Errors.ENOENT) {
        // currently no available version
        FileRecord record = record_map_.get(path);
        if (record != null) {
          record.SetReaderVersionId(-1);
          timestamp_map_.remove(path);
        }
      }
      if (error_code < 0) { // server already checks error for proxy
        Logger.Log("error cdode < 0 = " + error_code);
        return new OpenReturnVal(null, error_code, if_directory);
      }
      long server_file_timestamp = validate_result.timestamp;
      byte[] file_data = validate_result.data;
      if (cache_file_timestamp != server_file_timestamp && server_file_timestamp >= 0
          && file_data != null) {
        // new content is updated from the server side, save it
        SaveData(path, file_data, server_file_timestamp);
      }

      if (if_directory) {
        // the dummy read directory command
        return new OpenReturnVal(null, cache_fd_++, if_directory);
      }
      // create new entry in the record map if necessary
      FileRecord record = record_map_.get(path);
      if (record == null) {
        int init_version = -1;
        record = new FileRecord(path, init_version, init_version);
        record_map_.put(path, record);
      }
      return GetAndRegisterFile(record, path, option);
    } catch (FileSystemException | FileNotFoundException e) {
      // already check for filenotfound above, assume it is permission problem
      e.printStackTrace();
      return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
    } catch (IOException e) {
      // let EIO=5 be the indicator for IOException for now
      e.printStackTrace();
      return new OpenReturnVal(null, EIO, false);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      mtx_.unlock();
    }
    // dummy placeholder for uncaught unknown exception
    return new OpenReturnVal(null, EIO, false);
  }

  public int close(int fd) {
    mtx_.lock();
    try {
      if (!fd_handle_map_.containsKey(fd)) {
        return FileHandling.Errors.EBADF;
      }
      DeregisterFile(fd); // include upload file to server
      return 0;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      mtx_.unlock();
    }
    return -1; // indicate any other form of error
  }

  public int unlink(String path) {
    mtx_.lock();
    try {
      int code = remote_manager_.Delete(path);
      if (code == 0) {
        // delete on server side is successful
        FileRecord record = record_map_.get(path);
        if (record != null) {
          record.SetReaderVersionId(-1);
          timestamp_map_.remove(path);
        }
      }
      return code;
    } catch (SecurityException e) {
      e.printStackTrace();
      return FileHandling.Errors.EPERM;
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      mtx_.unlock();
    }
    return EIO; // indicate any other form of error
  }
}