/**
 * file: FileChecker.java
 * author: Yukun Jiang
 * date: Feb 12
 *
 * This is Interface supported to implement the "check-on-use" semantics
 * between server and proxy
 * */
import java.io.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
public interface FileChecker {
  public boolean IfExist(String path);

  public boolean IfDirectory(String path);

  public boolean IfRegularFile(String path);

  public boolean IfCanRead(String path);

  public boolean IfCanWrite(String path);

  public ValidateResult Validate(String path, FileHandling.OpenOption option, long timestamp);
}
