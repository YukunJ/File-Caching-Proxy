/**
 * file: Cache.java
 * author: Yukun Jiang
 * date: Feb 08
 *
 * This is the underlying cache management
 * It needs to control the overall size of cached file
 *
 * and to retain session semantics, it will return a
 * RandomAccessFile to Proxy upon request, and depending on whether
 * it's read mode or write mode, may create a new temp file or use old file
 *
 * File are represented as FileRecord, which records the version number of one
 * file and keep reference count of each version and delete the version when
 * refernce count drops to zero.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
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

  public int GetRefCount() { return ref_count_; }

  public int PlusRefCount() { return ++ref_count_; }

  public int MinusRefCount() { return --ref_count_; }

  public String ToFileName() {
    return filename_ + ((version_ == 0) ? "" : Integer.toString(version_));
  }
}

class FileRecord {
  public static int INITIAL_VERSION = 0;

  public static int NON_EXIST_VERSION = -1;

  public static int NON_REFERENCE = 0;

  public static int TIMESTAMP_INDEX = 0;

  public static int CHUNK_INDEX = 1;
  private final String filename_;

  public final HashMap<Integer, Version> version_map_;

  /* the latest reader-visible version, if -1 means no one can see this file now
   */
  private int reader_version_;

  /* the latest spawn version by writer, monotonically increasing */
  private int latest_version_;

  /* if the file already exists on disk, reader_version is init to 0
     if new writer create this file, reader_version is init to -1 so no one sees
     it until commit
   */
  public FileRecord(String filename, int reader_version, int latest_version) {
    filename_ = filename;
    reader_version_ = reader_version;
    latest_version_ = latest_version;
    version_map_ = new HashMap<>();
    if (reader_version == INITIAL_VERSION) {
      version_map_.put(INITIAL_VERSION, new Version(filename, INITIAL_VERSION));
    }
  }

  /**
   * Get a shared copy of the reader version of this file
   * caller should ensure that reader_version >= 0 already
   */
  public FileReturnVal GetReaderFile() throws Exception {
    int reader_version_id = GetReaderVersionId();
    Version reader_version = GetReaderVersion();
    String reader_filename = reader_version.ToFileName();
    String cache_reader_filepath = Cache.FormatPath(reader_filename);
    RandomAccessFile file_handle =
        new RandomAccessFile(cache_reader_filepath, Cache.READER_MODE);
    reader_version.PlusRefCount();
    Cache.HitFileInLRUCache(reader_version);
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
    if (GetReaderVersionId() >= INITIAL_VERSION) {
      // there is existing version, copy it
      String reader_filename = GetReaderVersion().ToFileName();
      GetReaderVersion()
          .PlusRefCount(); // temporarily protect this reader file, do not evict
      String cache_reader_filepath = Cache.FormatPath(reader_filename);
      boolean success = Cache.ReserveCacheSpace(
          new File(cache_reader_filepath).length(), false);
      if (!success) {
        GetReaderVersion().MinusRefCount();
        return new FileReturnVal(null, FileHandling.Errors.ENOMEM);
      }
      CopyFile(cache_writer_filepath, cache_reader_filepath);
      GetReaderVersion().MinusRefCount();
    }
    Cache.HitFileInLRUCache(writer_version);
    RandomAccessFile file_handle =
        new RandomAccessFile(cache_writer_filepath, Cache.WRITER_MODE);
    version_map_.put(writer_version_id,
                     writer_version); // must be an exclusive version
    writer_version.PlusRefCount();
    return new FileReturnVal(file_handle, writer_version_id);
  }

  /**
   * Close a shared reader version of this file
   * and additionally clean up if no one else will see/use this version
   */
  public void CloseReaderFile(int version_id) {
    Version reader_version = version_map_.get(version_id);
    Cache.HitFileInLRUCache(reader_version);
    int remain_ref_count = reader_version.MinusRefCount();
    if (remain_ref_count == NON_REFERENCE &&
        version_id != GetReaderVersionId()) {
      // no more client will see this version
      // version_map_.remove(version_id);
      version_map_.remove(version_id);
      Cache.EvictCacheEntry(reader_version);
    }
  }

  /**
   * Close an exclusive version of this file
   * and install this to be the newest visible reader version
   */
  public void CloseWriterFile(int version_id) {
    Version writer_version = version_map_.get(version_id);
    Cache.HitFileInLRUCache(writer_version);
    byte[] data;
    try {
      // must be 0 now
      writer_version.MinusRefCount();
      RandomAccessFile file = new RandomAccessFile(
          Cache.FormatPath(writer_version.ToFileName()), Cache.READER_MODE);
      Integer max_chunk_size = FileChunk.CHUNK_SIZE;
      Integer file_remain_size = (int)(file.length() - file.getFilePointer());
      Integer chunk_size = Math.min(max_chunk_size, file_remain_size);
      Boolean is_end = (file_remain_size <= max_chunk_size);
      data = new byte[chunk_size];
      file.read(data);
      // upload new version to server iteratively chunk-by-chunk and record the
      // timestamp
      String origin_filename = writer_version.filename_;
      Long[] tuple = Cache.remote_manager_.Upload(
          origin_filename, new FileChunk(data, is_end, NON_EXIST_VERSION));
      Long server_timestamp = tuple[TIMESTAMP_INDEX];
      Integer chunk_id = tuple[CHUNK_INDEX].intValue();
      while (!is_end) {
        file_remain_size = (int)(file.length() - file.getFilePointer());
        chunk_size = Math.min(max_chunk_size, file_remain_size);
        is_end = (file_remain_size <= max_chunk_size);
        data = new byte[chunk_size];
        file.read(data);
        Cache.remote_manager_.UploadChunk(
            new FileChunk(data, is_end, chunk_id));
      }
      file.close();
      // install to be available new reader version
      int install_version_id = writer_version.version_;
      if (GetReaderVersionId() >= INITIAL_VERSION) {
        // there is existing reader version to be overwritten
        Version reader_version = GetReaderVersion();
        if (reader_version.GetRefCount() == NON_REFERENCE) {
          // there will be no one able to point to this reader version anymore
          Cache.EvictCacheEntry(reader_version);
          version_map_.remove(reader_version.version_);
        }
      }
      SetReaderVersionId(install_version_id);
      // should not remove this versilename, server_timestamp);on from map,
      // future reader need it
      Cache.UpdateTimestamp(origin_filename, server_timestamp);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int IncrementLatestVersionId() { return ++latest_version_; }
  public int GetLatestVersionId() { return latest_version_; }

  public int GetReaderVersionId() { return reader_version_; }

  public Version GetReaderVersion() {
    return version_map_.get(reader_version_);
  }
  public void SetReaderVersionId(int new_reader_version) {
    reader_version_ = new_reader_version;
  }

  /**
   * copy the file specified by src_name into a new file named by dest_name
   */
  public static void CopyFile(String dest_name, String src_name)
      throws Exception {
    File src = new File(src_name);
    File dest = new File(dest_name);
    Files.copy(src.toPath(), dest.toPath(),
               StandardCopyOption.REPLACE_EXISTING);
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
  private static final HashMap<String, FileRecord> record_map_ =
      new HashMap<>();
  private final HashMap<Integer, RandomAccessFile> fd_handle_map_;
  private final HashMap<Integer, String> fd_filename_map_;

  private final HashMap<Integer, Integer> fd_version_map_;

  private final HashMap<Integer, FileHandling.OpenOption> fd_option_map_;

  /* the lru freshness ordering of all versions of cached file in this local
   * Cache Proxy */
  public final static LinkedHashSet<Version> lru_ = new LinkedHashSet<>();

  private static Long cache_occupancy_ = 0L;

  private static Long cache_capacity_ = 0L;

  private final ReentrantLock mtx_;

  private static final int ZERO = 0;

  private static final int SUCCESS = 0;

  private static final int FAILURE = -1;

  private final static ReentrantLock cache_mtx_ = new ReentrantLock();
  private static String cache_dir_;

  public Cache() {
    cache_fd_ = INIT_FD;
    fd_handle_map_ = new HashMap<>();
    fd_filename_map_ = new HashMap<>();
    fd_version_map_ = new HashMap<>();
    fd_option_map_ = new HashMap<>();
    mtx_ = new ReentrantLock();
  }

  public static void UpdateTimestamp(String path, Long timestamp) {
    timestamp_map_.put(path, timestamp);
  }

  public void SetCacheCapacity(Long capacity) { cache_capacity_ = capacity; }

  public long GetCacheOccupancy() { return cache_occupancy_; }

  /*
     register/update a whole filepath into the LRU cache
     size update should be done separately
   */
  public static void HitFileInLRUCache(Version file_version) {
    // try remove first to update its freshness position
    RemoveFileFromLRUCache(file_version);
    lru_.add(file_version);
  }

  public static void RemoveFileFromLRUCache(Version file_version) {
    lru_.remove(file_version);
  }

  public static void IncreaseCacheOccupancy(Long size) {
    cache_occupancy_ += size;
  }

  public static void DecreaseCacheOccupancy(Long size) {
    cache_occupancy_ -= size;
  }

  /* Reserve a certain space from cache
     Upon success, 'size' space will be reserved for the caller to use and
     return True If no space is available, it will try evict a few other entries
     if possible

     However, if all entries are in use, or not space available possible
     (resever ~100GB file) It returns False
   */
  public static boolean ReserveCacheSpace(Long size, boolean should_lock) {
    if (should_lock) {
      cache_mtx_.lock();
    }
    try {
      long remain = cache_capacity_ - cache_occupancy_;
      if (remain >= size) {
        IncreaseCacheOccupancy(size);
        return true;
      }
      // current remain space is not enough, need to evict
      while (EvictOneCacheEntry()) {
        remain = cache_capacity_ - cache_occupancy_;
        if (remain >= size) {
          IncreaseCacheOccupancy(size);
          return true;
        }
      }
      return false;
    } finally {
      if (should_lock) {
        cache_mtx_.unlock();
      }
    }
  }

  /* Remove a specific file version from both disk and cache entry
     typically happens when pruning so that no client will ever see a stale
     cached version of a file
   */
  public static void EvictCacheEntry(Version file_version) {
    String full_path = Cache.FormatPath(file_version.ToFileName());
    lru_.remove(file_version);
    DecreaseCacheOccupancy(DeleteFile(full_path));
    FileRecord record = record_map_.get(file_version.filename_);
    if (record.GetReaderVersionId() == file_version.version_) {
      // the reader version is masked off
      record_map_.get(file_version.filename_)
          .SetReaderVersionId(FileRecord.NON_EXIST_VERSION);
      UpdateTimestamp(file_version.ToFileName(), CACHE_NO_EXIST);
    }
  }

  /* Try to evict one entry from LRU Cache by LRU policy
     if no entry can be removed, return False
   */
  public static boolean EvictOneCacheEntry() {
    if (lru_.isEmpty()) {
      return false;
    }
    for (Version file_version : lru_) {
      if (file_version.GetRefCount() > FileRecord.NON_REFERENCE) {
        // some other clients are using it, cannot evcit yet
        continue;
      }
      String full_path = Cache.FormatPath(file_version.ToFileName());
      int reader_version_id =
          record_map_.get(file_version.filename_).GetReaderVersionId();
      lru_.remove(file_version);
      if (reader_version_id == file_version.version_) {
        // the reader version is masked off
        record_map_.get(file_version.filename_)
            .SetReaderVersionId(FileRecord.NON_EXIST_VERSION);
        UpdateTimestamp(file_version.ToFileName(), CACHE_NO_EXIST);
      }
      long freed_space = DeleteFile(full_path);
      DecreaseCacheOccupancy(freed_space);
      return true;
    }
    return false;
  }

  /* set the cache root directory for disk storage */
  public void SetCacheDirectory(String cache_dir) { cache_dir_ = cache_dir; }

  /* add the handler to enable communicating with Server */
  public void AddRemoteFileManager(FileManagerRemote remote_manager) {
    this.remote_manager_ = remote_manager;
  }

  /**
   * delete a file specified by the name
   * and return the deleted file's size for adjusting cache storage
   */
  public static long DeleteFile(String name) {
    File file = new File(name);
    try {
      long size = file.length();
      boolean success = file.delete();
      return size;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return SUCCESS;
  }

  /* do book-keeping after a successful open */
  public int Register(String filename, RandomAccessFile file_handle,
                      int version, FileHandling.OpenOption option) {
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
  public OpenReturnVal GetAndRegisterFile(FileRecord record, String path,
                                          FileHandling.OpenOption option)
      throws Exception {
    FileReturnVal return_val;
    if (option == FileHandling.OpenOption.READ) {
      return_val = record.GetReaderFile();
    } else {
      return_val = record.GetWriterFile();
      if (return_val.version_ == FileHandling.Errors.ENOMEM) {
        // no available space to make a writer copy
        return new OpenReturnVal(null, FileHandling.Errors.ENOMEM, false);
      }
    }
    RandomAccessFile file_handle = return_val.file_handle_;
    int version = return_val.version_;
    // register for bookkeeping
    int fd = Register(path, file_handle, version, option);
    return new OpenReturnVal(file_handle, fd, false);
  }

  /* Upon closing a fd, remove it from mapping record and upload to server new
   * version of necessary
   */
  public void DeregisterFile(int fd) throws Exception {
    RandomAccessFile file_handle = fd_handle_map_.get(fd);
    int version_id = fd_version_map_.get(fd);
    FileHandling.OpenOption option = fd_option_map_.get(fd);
    String filename = fd_filename_map_.get(fd);
    FileRecord record = record_map_.get(filename);
    file_handle.close();
    // need to physically close this file
    // before make it visible to other threads
    mtx_.lock();
    if (option == FileHandling.OpenOption.READ) {
      record.CloseReaderFile(version_id);
    } else {
      record.CloseWriterFile(version_id);
    }
    mtx_.unlock();
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
  private boolean SaveData(String path, FileChunk chunk,
                           Long server_timestamp) {
    try {
      FileRecord record = record_map_.get(path);
      if (record == null) {
        record = new FileRecord(path, FileRecord.NON_EXIST_VERSION,
                                FileRecord.NON_EXIST_VERSION);
        record_map_.put(path, record);
      } else {
        // check if there is an available reader version for this file
        // if so, actively evict it since we know it's stale and are downloading
        // a new version
        if (record.GetReaderVersionId() >= FileRecord.INITIAL_VERSION) {
          Version curr_reader_version = record.GetReaderVersion();
          if (curr_reader_version.GetRefCount() == FileRecord.INITIAL_VERSION) {
            Cache.EvictCacheEntry(curr_reader_version);
            record.version_map_.remove(curr_reader_version.version_);
            record.SetReaderVersionId(FileRecord.NON_EXIST_VERSION);
          }
        }
      }
      int version_id = record.IncrementLatestVersionId();
      Version version = new Version(path, version_id);
      record.version_map_.put(version_id, version);
      version.PlusRefCount(); // act as a client temporarily, no evict on this
                              // version
      HitFileInLRUCache(version);

      String cache_path = FormatPath(version.ToFileName());
      File directory =
          new File(new File(cache_path).getParentFile().getAbsolutePath());
      directory.mkdirs();
      RandomAccessFile file = new RandomAccessFile(cache_path, WRITER_MODE);
      file.setLength(ZERO); // clear off content
      while (true) {
        // while downloading this chunk, reserve space from Cache
        boolean success = ReserveCacheSpace((long)chunk.data.length, false);
        if (!success) {
          // cannot store this big file into cache space
          version.MinusRefCount();
          EvictCacheEntry(version); // deallocate space
          record.version_map_.remove(version_id);
          if (!chunk.end_of_file) {
            // server side holds a reader lock for you, cancel it
            remote_manager_.CancelChunk(chunk.chunk_id);
          }
          return false;
        }
        file.write(chunk.data);
        if (chunk.end_of_file) {
          break;
        } else {
          chunk = remote_manager_.DownloadChunk(chunk.chunk_id);
        }
      }
      version.MinusRefCount(); // finish writing into this file
      file.close();
      UpdateTimestamp(
          path,
          server_timestamp); // save the server timestamp for original path
      record.SetReaderVersionId(
          version_id); // make this version available to clients
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * Proxy delegate the open functionality to cache
   * and cache make local disk operations based on check-on-use results from the
   * server it should properly handle Exception and return corresponding error
   * code if applicable
   */
  public OpenReturnVal open(String path, FileHandling.OpenOption option) {
    boolean locked = false;
    try {
      long cache_file_timestamp =
          timestamp_map_.getOrDefault(path, CACHE_NO_EXIST);
      /* send validation request to server */
      ValidateResult validate_result = remote_manager_.Validate(
          new ValidateParam(path, option, cache_file_timestamp));
      int error_code = validate_result.error_code;
      boolean if_directory = validate_result.is_directory;
      if (error_code == FileHandling.Errors.ENOENT) {
        // currently no available version
        FileRecord record = record_map_.get(path);
        if (record != null) {
          record.SetReaderVersionId(FileRecord.NON_EXIST_VERSION);
          timestamp_map_.remove(path);
        }
      }
      if (error_code < SUCCESS) { // server already checks error for proxy
        return new OpenReturnVal(null, error_code, if_directory);
      }
      if (if_directory) {
        // the dummy read directory command
        return new OpenReturnVal(null, cache_fd_++, if_directory);
      }
      mtx_.lock();
      locked = true;
      long server_file_timestamp = validate_result.timestamp;
      FileChunk file_chunk = validate_result.chunk;
      if (server_file_timestamp >= Server.SERVER_NO_EXIST &&
          file_chunk != null) {
        if (timestamp_map_.getOrDefault(path, CACHE_NO_EXIST) ==
            server_file_timestamp) {
          // someone else already downloaded for us meanwhile
          if (!file_chunk.end_of_file) {
            // server is holding a lock for this file, no need
            remote_manager_.CancelChunk(file_chunk.chunk_id);
          }
        } else {
          // new content is updated from the server side, save it
          // iteratively ask for more chunks from server until EOF
          boolean success = SaveData(path, file_chunk, server_file_timestamp);
          if (!success) {
            return new OpenReturnVal(null, FileHandling.Errors.ENOMEM,
                                     if_directory);
          }
        }
      }
      // create new entry in the record map if necessary
      FileRecord record = record_map_.get(path);
      if (record == null) {
        record = new FileRecord(path, FileRecord.NON_EXIST_VERSION,
                                FileRecord.NON_EXIST_VERSION);
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
      if (locked) {
        mtx_.unlock();
      }
    }
    // dummy placeholder for uncaught unknown exception
    return new OpenReturnVal(null, EIO, false);
  }

  /**
   * Close a file descriptor and potentially upload new modification to Server
   */
  public int close(int fd) {
    if (!fd_handle_map_.containsKey(fd)) {
      return FileHandling.Errors.EBADF;
    }
    try {
      DeregisterFile(fd); // include upload file to server and cache pruning
      return SUCCESS;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return FAILURE; // indicate any other form of error
  }

  /**
   * Remove a file by signaling to Server such intent
   */
  public int unlink(String path) {
    try {
      int code = remote_manager_.Delete(path);
      if (code == SUCCESS) {
        // delete on server side is successful
        mtx_.lock();
        FileRecord record = record_map_.get(path);
        if (record != null) {
          record.SetReaderVersionId(FileRecord.NON_EXIST_VERSION);
          timestamp_map_.remove(path);
          ArrayList<Version> evict_candidates = new ArrayList<>();
          for (Version v : record.version_map_.values()) {
            if (v.GetRefCount() == FileRecord.NON_REFERENCE) {
              // no one is currently using this version and its whole deleted
              evict_candidates.add(v);
            }
          }
          for (Version v : evict_candidates) {
            EvictCacheEntry(v);
          }
        }
        mtx_.unlock();
      }
      return code;
    } catch (SecurityException e) {
      e.printStackTrace();
      return FileHandling.Errors.EPERM;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return EIO; // indicate any other form of error
  }
}