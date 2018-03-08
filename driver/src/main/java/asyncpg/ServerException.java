package asyncpg;

public class ServerException extends RuntimeException {
  public final Notice notice;

  public ServerException(Notice notice) {
    super(notice.toString());
    this.notice = notice;
  }
}
