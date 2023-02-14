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
  boolean IfExist(String path);

  boolean IfDirectory(String path);

  boolean IfRegularFile(String path);

  boolean IfCanRead(String path);

  boolean IfCanWrite(String path);

  ValidateResult Validate(String path);
}
