/**
 * file: FileManagerRemote.java
 * author: Yukun Jiang
 * date: Feb 14
 *
 * This is Interface shared between the Proxy and Server to use Java RMI
 * to communicate and validate the file cache state between two sites
 * */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.ServerNotActiveException;

/*
  The interface shared between Proxy and Server
  See detailed implementation in Server.Java
 */
public interface FileManagerRemote extends Remote {
  public static final String SERVER_NAME = "FileServer";

  public ValidateResult Validate(ValidateParam param) throws RemoteException;

  public FileChunk DownloadChunk(Integer chunk_id) throws RemoteException, IOException;

  public Long[] Upload(String path, FileChunk chunk) throws RemoteException, IOException;

  public void UploadChunk(FileChunk chunk) throws RemoteException, IOException;

  public void CancelChunk(Integer chunk_id) throws RemoteException;

  public int Delete(String path) throws RemoteException;
}
