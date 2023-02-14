/**
 * file: Server.java
 * author: Yukun Jiang
 * date: Feb 14
 *
 * This is the main file server that interact with multiple proxy
 * It implements the FileManagerRemote interface and provide
 * session semantics to its client
 * */

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.*;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;

public class Server extends UnicastRemoteObject implements FileManagerRemote {
  public Server() throws RemoteException {
    super(0);
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

  @Override
  public long Upload(String path, byte[] data) throws RemoteException {
    return 0;
  }

  @Override
  public int Delete(String path) throws RemoteException {
    return 0;
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
    Server server = new Server();

    // add reference to registry so clients can find it using name FileServer
    String address = "//127.0.0.1:" + args[0] + "/" + FileManagerRemote.SERVER_NAME;
    Naming.rebind(address, server);
  }
}
