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
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple logging tool print to stderr
 */
class Logger {
  private static int count = 0;
  private static final int LOG_MAX = 1000;
  public static void Log(String msg) {
    // tunable parameter to control the maximum amount of LOGGING
    if (count++ > LOG_MAX) {
      return;
    }
    System.err.println("###Logger### " + msg);
  }
  public static String OpenOptionToString(FileHandling.OpenOption option) {
    if (option == FileHandling.OpenOption.READ) {
      return "READ";
    }
    if (option == FileHandling.OpenOption.CREATE) {
      return "CREATE";
    }
    if (option == FileHandling.OpenOption.CREATE_NEW) {
      return "CREATE_NEW";
    }
    if (option == FileHandling.OpenOption.WRITE) {
      return "WRITE";
    }
    return "Unknown";
  }

  public static String SeekOptionToString(FileHandling.LseekOption option) {
    if (option == FileHandling.LseekOption.FROM_CURRENT) {
      return "FROM_CURRENT";
    }
    if (option == FileHandling.LseekOption.FROM_START) {
      return "FROM_START";
    }
    if (option == FileHandling.LseekOption.FROM_END) {
      return "FROM_END";
    }
    return "Unknown";
  }
}

/* For Cache returns to Proxy */
class OpenReturnVal {
  /* if success, a RandomAccessFile handle is returned */
  public RandomAccessFile file_handle_;
  /* might be negative to indicate error */
  public int fd_;

  OpenReturnVal(RandomAccessFile file_handle, int fd) {
    file_handle_ = file_handle;
    fd_ = fd;
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

class FileRecord {
  /**
   * The version info about a file
   * one file might have different versions at the same time due to concurrency
   */
  class Version {
    private final int version_;
    private final String filename_;
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

  private final String filename_;

  private final HashMap<Integer, Version> version_map_;

  /* the latest reader-visible version */
  private int reader_version_;

  /* the latest spawn version by writer */
  private int latest_version_;

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
    final String reader_mode = "r";
    int reader_version_id = GetReaderVersionId();
    Version reader_version = GetReaderVersion();
    String reader_filename = reader_version.ToFileName();
    RandomAccessFile file_handle = new RandomAccessFile(reader_filename, reader_mode);
    Logger.Log("GerReaderFile for " + filename_ + " with reader_version=" + reader_version_id);
    reader_version.PlusRefCount();
    return new FileReturnVal(file_handle, reader_version_id);
  }

  /**
   * Get an exclusive writer copy of this file
   * if an existing version exist, make a copy and start from there
   * otherwise create an empty file to work with
   */
  public FileReturnVal GetWriterFile() throws Exception {
    final String writer_mode = "rw";
    int writer_version_id = IncrementLatestVersionId();
    Version writer_version = new Version(filename_, writer_version_id);
    String writer_filename = writer_version.ToFileName();
    Logger.Log(
        "GetWriteFile() for " + filename_ + " with writer_version=" + writer_version.version_);
    if (GetReaderVersionId() >= 0) {
      // there is existing version, copy it
      String reader_filename = GetReaderVersion().ToFileName();
      CopyFile(writer_filename, reader_filename);
      Logger.Log("Make a copy from " + reader_filename + " to " + writer_filename);
    }
    RandomAccessFile file_handle = new RandomAccessFile(writer_filename, writer_mode);
    version_map_.put(writer_version_id, writer_version);
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
      version_map_.remove(version_id);
      // TODO: remove this version's associated disk file to clear cache space
    }
  }

  /**
   * Close an exclusive version of this file
   * and install this to be the newest visible reader version
   */
  public void CloseWriterFile(int version_id) {
    Version writer_version = version_map_.get(version_id);
    // must be 0 now
    assert (writer_version.MinusRefCount() == 0);
    // install to be available new reader version
    int install_version_id = writer_version.version_;
    SetReaderVersionId(install_version_id);
    // should not remove this version from map, future reader need it
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
  /* regular file descriptor offset */
  private static final int INIT_FD = 1024;
  private static final int EIO = -5;
  private int cache_fd_;
  private final HashMap<String, FileRecord> record_map_;
  private final HashMap<Integer, RandomAccessFile> fd_handle_map_;
  private final HashMap<Integer, String> fd_filename_map_;

  private final HashMap<Integer, Integer> fd_version_map_;

  private final HashMap<Integer, FileHandling.OpenOption> fd_option_map_;

  private final ReentrantLock mtx_;

  public Cache() {
    cache_fd_ = INIT_FD;
    record_map_ = new HashMap<>();
    fd_handle_map_ = new HashMap<>();
    fd_filename_map_ = new HashMap<>();
    fd_version_map_ = new HashMap<>();
    fd_option_map_ = new HashMap<>();
    mtx_ = new ReentrantLock();
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
    // register book-keeping
    int fd = Register(path, file_handle, version, option);
    return new OpenReturnVal(file_handle, fd);
  }

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

  /**
   * Proxy delegate the open functionality to cache
   * it should properly handle Exception and return corresponding error code if applicable
   */
  public OpenReturnVal open(String path, FileHandling.OpenOption option) {
    mtx_.lock();
    try {
      if (option == FileHandling.OpenOption.READ) {
        // existing read only
        FileRecord record = record_map_.get(path);
        if (record != null) {
          // has a record, not necessarily valid for read
          int reader_version = record.GetReaderVersionId();
          if (reader_version < 0) {
            // equivalent to file not found
            return new OpenReturnVal(null, FileHandling.Errors.ENOENT);
          } else {
            // grab a shared copy of reader version
            return GetAndRegisterFile(record, path, option);
          }
        } else {
          // fresh read, check existence and create FileRecord
          File f = new File(path);
          if (!f.exists()) {
            // file not exist
            return new OpenReturnVal(null, FileHandling.Errors.ENOENT);
          }
          if (!f.isFile()) {
          }
          if (!f.canRead()) {
            // permission problem
            return new OpenReturnVal(null, FileHandling.Errors.EPERM);
          }
          // create FileRecord for it
          record = new FileRecord(path, 0, 0);
          record_map_.put(path, record);
          return GetAndRegisterFile(record, path, option);
        }
      } else {
        // other 3 modifying operations
        if (option == FileHandling.OpenOption.CREATE_NEW) {
          // possibility 1: already exists on disk and it's first access
          if (!record_map_.containsKey(path) && new File(path).exists()) {
            return new OpenReturnVal(null, FileHandling.Errors.EEXIST);
          }
          // possibility 2: other writer has created it but not close yet
          if (record_map_.containsKey(path) && record_map_.get(path).GetReaderVersionId() >= 0) {
            return new OpenReturnVal(null, FileHandling.Errors.EEXIST);
          }
        }
        if (option == FileHandling.OpenOption.WRITE) {
          if (!record_map_.containsKey(path)) {
            // first access
            if (!(new File(path).exists())) {
              // not exist
              return new OpenReturnVal(null, FileHandling.Errors.ENOENT);
            }
            if (!(new File(path).isFile())) {
              // not a regular file
              return new OpenReturnVal(null, FileHandling.Errors.EPERM);
            }
            if (!(new File(path).canWrite())) {
              // permission problem
              return new OpenReturnVal(null, FileHandling.Errors.EPERM);
            }
          } else {
            if (record_map_.get(path).GetReaderVersionId() < 0) {
              // some writer has created this file but should not be visible now
              return new OpenReturnVal(null, FileHandling.Errors.ENOENT);
            }
            if (!(new File(record_map_.get(path).GetReaderVersion().ToFileName())).canWrite()) {
              return new OpenReturnVal(null, FileHandling.Errors.EPERM);
            }
          }
        }
        // TODO: duplicate code from above branch, try to modularize this
        FileRecord record = record_map_.get(path);
        if (record == null) {
          if (new File(path).exists()) {
            // enable write-copy from the existing reader version
            record = new FileRecord(path, 0, 0);
          } else {
            record = new FileRecord(path, -1, -1);
          }
          record_map_.put(path, record);
        }
        return GetAndRegisterFile(record, path, option);
      }
    } catch (FileSystemException | FileNotFoundException e) {
      // already check for filenotfound above, assume it is permission problem
      e.printStackTrace();
      return new OpenReturnVal(null, FileHandling.Errors.EPERM);
    } catch (IOException e) {
      // let EIO=5 be the indicator for IOException for now
      e.printStackTrace();
      return new OpenReturnVal(null, EIO);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      mtx_.unlock();
    }
    // dummy placeholder for uncaught unknown exception
    return new OpenReturnVal(null, -1);
  }

  public int close(int fd) {
    mtx_.lock();
    try {
      if (!fd_handle_map_.containsKey(fd)) {
        return FileHandling.Errors.EBADF;
      }
      DeregisterFile(fd);
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
      // first check if this is a directory
      File file = new File(path);
      if (file.exists() && file.isDirectory()) {
        return FileHandling.Errors.EISDIR;
      }
      // divide into cases by if the record exists for this file
      FileRecord record = record_map_.get(path);
      if (record != null) {
        // so readers will not be able to see it, until any writer returns
        record.SetReaderVersionId(-1);
        return 0;
        // TODO: in cpkt2&3 actually need to delete the file, need some kind of reference counting
      } else {
        if (!file.exists()) {
          return FileHandling.Errors.ENOENT;
        }
        boolean res = file.delete();
        return 0;
      }
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

  public static void main(String[] args) {
    System.out.println("Init Cache");
  }
}