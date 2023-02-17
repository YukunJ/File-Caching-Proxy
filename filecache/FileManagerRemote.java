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

public interface FileManagerRemote extends Remote {
  public static final String SERVER_NAME = "FileServer";

  public ValidateResult Validate(ValidateParam param) throws RemoteException;

  public long Upload(String path, byte[] data) throws RemoteException, IOException;

  public int Delete(String path) throws RemoteException;
}
