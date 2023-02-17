/**
 * Simple logging tool print to stderr for debug purpose
 */
public class Logger {
  private static int count = 0;
  private static final int LOG_MAX = 1000;

  public static void Log(String msg) {
    // tunable parameter to control the maximum amount of LOGGING
    if (count++ > LOG_MAX) {
      return;
    }
    System.err.println("###Logging###: " + msg);
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
