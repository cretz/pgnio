package pgnio;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class SubscribableQueryTest extends DbTestBase {
  @Test
  public void testNotifications() throws Exception {
    BlockingQueue<Subscribable.Notification> notifications = new ArrayBlockingQueue<>(20);
    AtomicBoolean listenStopAfterReceipt = new AtomicBoolean();

    // Listen until stopped, 30 seconds on tick before timeout
    CompletableFuture<Void> listen = Connection.authed(db.conf().dbConf).thenCompose(conn ->
      conn.terminated(conn.simpleQueryExec("LISTEN test_notifications").thenCompose(__ -> {
        conn.notifications().subscribe(notification -> {
          try {
            notifications.put(notification);
            if (listenStopAfterReceipt.get()) return CompletableFuture.completedFuture(null);
            return conn.unsolicitedMessageTick(30, TimeUnit.SECONDS);
          } catch (InterruptedException e) { throw new RuntimeException(e); }
        });
        return conn.unsolicitedMessageTick(30, TimeUnit.SECONDS);
      })));

    // Notify
    int notifyProcessId = withConnectionSync(conn ->
      conn.simpleQueryExec("NOTIFY test_notifications").
          thenCompose(c -> c.simpleQueryExec("NOTIFY test_notifications, 'test1'")).
          thenCompose(c -> {
            // Let's just wait until the size is 2 and then we'll prepare it to stop and send the next
            try {
              for (int i = 0; i < 20 && notifications.size() < 2; i++) Thread.sleep(50);
            } catch (InterruptedException e) { throw new RuntimeException(e); }
            Assert.assertEquals(2, notifications.size());
            listenStopAfterReceipt.set(true);
            return c.simpleQueryExec("NOTIFY test_notifications, 'test2'");
          })).getProcessId();
    // Wait for listen conn to exit and confirm notifications
    listen.get();
    List<Subscribable.Notification> expected = Arrays.asList(
        new Subscribable.Notification(notifyProcessId, "test_notifications", ""),
        new Subscribable.Notification(notifyProcessId, "test_notifications", "test1"),
        new Subscribable.Notification(notifyProcessId, "test_notifications", "test2")
    );
    Assert.assertEquals(expected, new ArrayList<>(notifications));
  }

  @Test
  public void testNotices() {
    AtomicReference<Subscribable.Notice> notice = new AtomicReference<>();
    withConnectionSync(conn -> {
      // Catch and set
      conn.notices().subscribe(n -> {
        Assert.assertTrue(notice.compareAndSet(null, n));
        return CompletableFuture.completedFuture(null);
      });
      // Just raise a simple notice which should come right back without tick
      return conn.simpleQueryExec(
          "DO language plpgsql $$\n" +
          "BEGIN\n" +
          "  RAISE NOTICE 'notice test';\n" +
          "END\n" +
          "$$;");
    });
    Assert.assertEquals("notice test", notice.get().getMessage());
  }

  @Test
  public void testParameterStatuses() {
    AtomicReference<Subscribable.ParameterStatus> status = new AtomicReference<>();
    withConnectionSync(conn -> {
      // Catch and set
      conn.parameterStatuses().subscribe(s -> {
        Assert.assertTrue(status.compareAndSet(null, s));
        return CompletableFuture.completedFuture(null);
      });
      // Change the timezone
      return conn.simpleQueryExec("SET TIME ZONE 'Europe/Rome'");
    });
    Assert.assertEquals(new Subscribable.ParameterStatus("TimeZone", "Europe/Rome"), status.get());
  }
}
