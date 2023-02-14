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
   * otherwise create an empty file to start work with
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
  /* Checkpoint 1 version of FileChecker, will be moved to server side in ckpt 2 & 3 */
  class CacheFileChecker implements FileChecker {
    @Override
    public boolean IfExist(String path) {
      FileRecord record = record_map_.get(path);
      if (record == null) {
        // check on local disk
        return new File(path).exists();
      } else {
        // -1 reader version indicates currently invisible
        return record.GetReaderVersionId() >= 0;
      }
    }

    @Override
    public boolean IfDirectory(String path) {
      return new File(path).isDirectory();
    }

    @Override
    public boolean IfRegularFile(String path) {
      FileRecord record = record_map_.get(path);
      if (record != null && record.GetReaderVersionId() >= 0) {
        return new File(record.GetReaderVersion().ToFileName()).isFile();
      } else {
        return new File(path).isFile();
      }
    }

    @Override
    public boolean IfCanRead(String path) {
      FileRecord record = record_map_.get(path);
      if (record != null && record.GetReaderVersionId() >= 0) {
        return new File(record.GetReaderVersion().ToFileName()).canRead();
      } else {
        return new File(path).canRead();
      }
    }

    @Override
    public boolean IfCanWrite(String path) {
      FileRecord record = record_map_.get(path);
      if (record != null && record.GetReaderVersionId() >= 0) {
        return new File(record.GetReaderVersion().ToFileName()).canWrite();
      } else {
        return new File(path).canWrite();
      }
    }

    @Override
    public ValidateResult Validate(String path, long validation_timestamp) {
      return new ValidateResult(IfExist(path), IfDirectory(path), IfRegularFile(path),
          IfCanRead(path), IfCanWrite(path), validation_timestamp);
    }
  }

  /* file descriptor offset */
  private static final int INIT_FD = 1024;
  private static final int EIO = -5;
  private static final Long CACHE_NO_EXIST = -1L;
  private FileManagerRemote remote_manager_; // to communicate with Server
  private int cache_fd_;
  private final FileChecker checker_;
  private final HashMap<String, FileRecord> record_map_;
  private final HashMap<Integer, RandomAccessFile> fd_handle_map_;
  private final HashMap<Integer, String> fd_filename_map_;

  private final HashMap<Integer, Integer> fd_version_map_;

  private final HashMap<Integer, FileHandling.OpenOption> fd_option_map_;

  private final ReentrantLock mtx_;

  public Cache() {
    cache_fd_ = INIT_FD;
    checker_ = new CacheFileChecker();
    record_map_ = new HashMap<>();
    fd_handle_map_ = new HashMap<>();
    fd_filename_map_ = new HashMap<>();
    fd_version_map_ = new HashMap<>();
    fd_option_map_ = new HashMap<>();
    mtx_ = new ReentrantLock();
  }

  public void foo() throws RemoteException, ServerNotActiveException {
    System.out.println("Server says: " + remote_manager_.EchoIntBack(123));
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

  /**
   * Proxy delegate the open functionality to cache
   * and cache make local disk operations based on check-on-use results from the server
   * it should properly handle Exception and return corresponding error code if applicable
   */
  public OpenReturnVal open(String path, FileHandling.OpenOption option) {
    mtx_.lock();
    try {
      ValidateResult check_result = checker_.Validate(path, 0); // hack for compiler
      boolean if_exist = check_result.exist;
      boolean if_directory = check_result.is_directory;
      boolean if_regular = check_result.is_regular_file;
      boolean if_can_read = check_result.can_read;
      boolean if_can_write = check_result.can_write;
      // aggregate all error cases across open mode
      if (!if_exist
          && (option != FileHandling.OpenOption.CREATE
              && option != FileHandling.OpenOption.CREATE_NEW)) {
        // the requested file not found
        return new OpenReturnVal(null, FileHandling.Errors.ENOENT, false);
      }
      if (if_exist && option == FileHandling.OpenOption.CREATE_NEW) {
        // cannot create new
        return new OpenReturnVal(null, FileHandling.Errors.EEXIST, false);
      }
      if (if_directory) {
        // the path actually points to a directory
        if (option != FileHandling.OpenOption.READ) {
          // can only apply open with read option on directory
          return new OpenReturnVal(null, FileHandling.Errors.EISDIR, true);
        }
        if (!if_can_read) {
          // no read permission with this directory
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, true);
        }
        // good to go, a dummy directory fd
        return new OpenReturnVal(null, cache_fd_++, true);
      }
      if (!if_regular) {
        // not a regular file
        if (option == FileHandling.OpenOption.READ || option == FileHandling.OpenOption.WRITE) {
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
      }
      if (!if_can_read) {
        // read permission not granted
        if (option == FileHandling.OpenOption.READ) {
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          // not creating a new one, but old one cannot be read
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
      }
      if (!if_can_write) {
        // write permission not granted
        if (option == FileHandling.OpenOption.WRITE) {
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
        if (if_exist && option == FileHandling.OpenOption.CREATE) {
          // not creating a new one, but old one cannot be written
          return new OpenReturnVal(null, FileHandling.Errors.EPERM, false);
        }
      }

      // create new entry in the record map if necessary
      FileRecord record = record_map_.get(path);
      if (record == null) {
        int init_version = (if_exist) ? 0 : -1;
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
      if (file.isDirectory()) {
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
}