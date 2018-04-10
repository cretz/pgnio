# PgNio

PgNio is an asynchronous PostgreSQL client for Java and the JVM. It was built to solve both simple and advanced Postgres
needs without being too opinionated or inflexible. Since all protocol features are supported callers can take advanced
approaches to problems.

[![Javadocs](http://javadoc.io/badge/com.github.cretz.pgnio/pgnio-driver.svg)](http://javadoc.io/doc/com.github.cretz.pgnio/pgnio-driver)
(`protected` visibility excluded)

### Features/Goals

* Java 8+
* Simple and small codebase, no dependencies
* NIO and SSL
* Notification support
* `COPY` support
* Advanced prepared and bound query support including max-rows and suspension
* Flexible server communication - control when/how data is sent/received, support all protocol features
* Extensible - almost everything can be extended
* Low level - can be the base of higher-level Postgres libraries (such as the upcoming async JDBC API or combined with
  other reactive libs)
* Extensible yet not embedded/forced parameter and row data serialization from/to Java types
* Support for date, geom, network, money, hstore, etc data types

### Install

This is deployed to Maven Central. In maven project:

```xml
<dependency>
    <groupId>com.github.cretz.pgnio</groupId>
    <artifactId>pgnio-driver</artifactId>
    <version>0.2.0</version>
</dependency>
```

Or in Gradle:

```
compile 'com.github.cretz.pgnio:pgnio-driver:0.2.0'
```

### How To

Below are simple examples on how to use the client library. The library makes heavy use of composition with
`CompletableFuture` values which is why some of the code appears quite functional and non-ergonomic. All top-level
classes in the library are in the `pgnio` package. While there are synchronous `get` invocations in these examples, in
normal use developers might not want to block for a result.

#### Create and use a single connection

To connect to a database, a `Config` instance is needed. It's a simple class with already-defaulted public fields that
can be set directly. Each field also has a corresponding builder method to set its value and return back the `Config`
instance. Here is a simple config:

```java
Config conf = new Config().hostname("myhost").username("myuser").password("mypass");
```

See the `Config` Javadoc for more details on the values. Besides common values, developers are encouraged to set timeout
values that are used for reading/writing from/to the server.

A connection is first connected via the `Connection.init` static method which accepts a `Config`. It is then
authenticated by calling auth on the resulting opened connection:

```java
Connection.init(conf).thenCompose(conn -> conn.auth()).thenCompose(conn -> /* do stuff */);
```

Note, this example does not block for response or close the connection but normal code would. A shortcut for this is to
just call `Connection.authed`.

Once the connection is done, just call `terminate` or pass a future to `terminated`:

```java
Connection.authed(conf).thenCompose(conn -> conn.terminated(/* do stuff returning future */)).get();
```

This example does block using `get` at the end. It also calls `terminated` to close the connection after stuff is done.
Here is an example of fetching a simple string from a query:

```java
List<QueryMessage.Row> rows = Connection.authed(conf).thenCompose(conn ->
    conn.terminated(conn.simpleQueryRows("SELECT current_database() AS database_name"))).get();
System.out.println("Current DB: " + RowReader.DEFAULT.get(rows.get(0), "database_name", String.class));
```

`RowReader` is covered later.

#### Create and use a connection pool

A `ConnectionPool` can be created with a `Config` like a connection and has a `withConnection` method that helps make
sure connections can be reused:

```java
ConnectionPool pool = new ConnectionPool(conf);
List<QueryMessage.Row> rows = pool.withConnection(conn ->
    conn.simpleQueryRows("SELECT current_database() AS database_name")).get();
System.out.println("Current DB: " + RowReader.DEFAULT.get(rows.get(0), "database_name", String.class));
```

The `Config`'s `poolSize` determines the fixed pool size. While not set by default, developers are encouraged to set
`Config.poolValidationQuery` to something like `SELECT 1` to make sure borrowed connections are always valid. A
`ConnectionPool` should be closed after use. For the rest of these examples, the `pool` variable above will be reused.

#### Execute simple queries

To execute a simple query and retrieve the query result connection state, use `simpleQuery`. This usage requires that
you mark the result `done`. There are convenience methods to do this automatically and return values. They are
`simpleQueryExec` for discarding the result, `simpleQueryRowCount` to get the returned/affected row count, and
`simpleQueryRows` to get the row list:

```java
pool.withConnection(c ->
    c.simpleQueryExec("CREATE TEMP TABLE foo (bar VARCHAR(100))").
        // The result is just of type java.lang.Void anyways, so ignore it
        thenCompose(__ -> c.simpleQueryRowCount("INSERT INTO foo VALUES ('test1'), ('test2')")).
        // The result is an integer, so this outputs "Rows: 2"
        thenAccept(rowCount -> System.out.println("Rows: " + rowCount)).
        // Now select em all
        thenCompose(__ -> c.simpleQueryRows("SELECT * FROM foo")).
        // Show the strings
        thenAccept(rows ->
            System.out.println("Rows: " + rows.stream().
                map(row -> RowReader.DEFAULT.get(row, "bar", String.class)).collect(Collectors.joining(", ")))
        )
).get();
```

#### Reading row values

Rows are returned as `QueryMessage.Row` objects. These objects include metadata about the columns and the two
dimensional byte array, with a byte array for each column. Instead of putting the logic to convert from byte arrays
inside the row class, PgNio offers a `RowReader` class for reading row data. The class may be manually instantiated
with custom converters, but most common uses will use the `RowReader.DEFAULT` singleton:

```java
pool.withConnection(c ->
    c.simpleQueryRows("SELECT 'test' AS first_row, 12, '{5, 6}'::integer[]").
        thenAccept(rows -> {
            // Pass in the row, column name, and type to fetch
            System.out.println("Col 1: " + RowReader.DEFAULT.get(rows.get(0), "first_row", String.class));
            // Can also pass in the zero-based column index
            System.out.println("Col 2: " + RowReader.DEFAULT.get(rows.get(0), 1, Integer.class));
            // Even works with arrays
            System.out.println("Col 3: " + Arrays.toString(RowReader.DEFAULT.get(rows.get(0), 2, int[].class)));
        })
).get();
```

See the Javadoc for more information on custom column value converters. See the [Data Types](#data-types) section below
for more information on supported data types.

#### Execute queries with parameters

In the [PostgreSQL protocol](https://www.postgresql.org/docs/current/static/protocol.html), there are two ways to submit
queries. One is the simple query form which issues a query and gets row metadata and row data. These are the calls
prefixed with "simple". The other way is the "advanced" or "prepared" approach which separates the steps to parse the
query, bind parameters, describe the result, and execute the query. The "simple" approach can be seen as just combining
those 4 steps together in one call on the server side. PgNio offers separate calls for each of these steps allowing the
caller to choose when/how they are called. There are also "prepared" convenience methods analogous to the "simple"
convenience methods which invoke all of these steps internally:

```java
pool.withConnection(c ->
    // Ask for a series from 1 through a parameter (4 in this case)
    c.preparedQueryRows("SELECT * FROM generate_series(1, $1)", 4).
        // Will be a count of 4
        thenAccept(rows -> System.out.println("Row count: " + rows.size()))
).get();
```

Internally, PgNio uses a `ParamWriter` instance (configured with a default via `Config.paramWriter`) to convert from
Java types to PostgreSQL parameters. See the [Data Types](#data-types) for more information on suggested data types for
certain parameter types.

#### Reuse prepared queries

The prepared queries above are "unnamed" (internally they use an empty string as the name) which means they can't easily
be reused. PgNio supports named prepared queries which are stored for the life of the connection or until closed. Unlike
unnamed prepared queries, there aren't convenience methods to create a named query, but convenience methods can be used
for binding, executing, and retrieving rows:

```java
pool.withConnection(c ->
    c.simpleQueryExec("CREATE TEMP TABLE foo (bar VARCHAR(100))").
        thenCompose(__ -> c.prepareReusable("myquery", "INSERT INTO foo VALUES ($1)")).
        // We would use bindDescribeExecuteAndDone if this were a select
        thenCompose(prepared -> prepared.bindExecuteAndDone("test1")).
        thenCompose(result -> result.done()).
        // Count will be 1
        thenCompose(__ -> c.simpleQueryRows("SELECT COUNT(1) FROM foo")).
        thenAccept(rows ->
            System.out.println("Count: " + RowReader.DEFAULT.get(rows.get(0), 0, Long.class))).
        // Reuse the query
        thenCompose(__ -> c.reusePrepared("myquery")).
        thenCompose(prepared -> prepared.bindExecuteAndDone("test2")).
        thenCompose(result -> result.done()).
        // Count will be 2
        thenCompose(__ -> c.simpleQueryRows("SELECT COUNT(1) FROM foo")).
        thenAccept(rows ->
            System.out.println("Count: " + RowReader.DEFAULT.get(rows.get(0), 0, Long.class))).
        // Try to close the statement regardless of error
        handle((__, ex) ->
            c.reusePrepared("myquery").
                thenCompose(prepared -> prepared.closeStatement()).
                thenCompose(prepared -> prepared.done()).
                thenCompose(result -> result.done()).
                thenAccept(___ -> { if (ex != null) throw new RuntimeException(ex); })).
        thenCompose(Function.identity())
).get();
```

Note, "life of the connection" means as long as the socket is open to the server. So when using a connection pool,
developers should always close their prepared statements or they will remain open as long as the connection does.

#### Use transactions

The regular "ready for query" connection state is the `QueryReadyConnection.AutoCommit` class which automatically
commits everything. Running `beginTransaction` on it returns a `QueryReadyConnection.InTransaction` class which won't
return back to auto commit mode until `commitTransaction` or `rollbackTransaction` is executed. Example:

```java
pool.withConnection(c ->
    c.simpleQueryExec("CREATE TEMP TABLE foo (bar VARCHAR(100))").
        // Start the transaction
        thenCompose(__ -> c.beginTransaction()).
        // Insert a value
        thenCompose(txn -> txn.simpleQueryExec("INSERT INTO foo VALUES ('test')").thenApply(__ -> txn)).
        // Count should be 1
        thenCompose(txn ->
            txn.simpleQueryRows("SELECT COUNT(1) FROM foo").thenApply(rows -> {
                System.out.println("Count: " + RowReader.DEFAULT.get(rows.get(0), 0, Long.class));
                return txn;
            })).
        // Roll it back
        thenCompose(txn -> txn.rollbackTransaction()).
        // Count should be 0
        thenCompose(conn -> conn.simpleQueryRows("SELECT COUNT(1) FROM foo")).
        thenAccept(rows ->
            System.out.println("Count: " + RowReader.DEFAULT.get(rows.get(0), 0, Long.class)))
).get();
```

Transactions can also be nested which is internally supported via savepoints.

#### Listen for notifications

PostgreSQL has `LISTEN`/`NOTIFY` support which allows pub/sub. PgNio allows subscription to these messages on a per
connection basis. Once subscribed to the messages, it must be read from the server side. This will happen during normal
query operations since a notification is sent along with other messages. But if not querying, developers need to wait
while reading for a message, which can be done via `unsolicitedMessageTick` and a timeout.

```java
// Create a listener
CompletableFuture listener = pool.withConnection(c -> {
    // Subscribe to the notification
    c.notifications().subscribe(notification -> {
        System.out.println("Got: " + notification.payload);
        // This function requires a future result so it can continue on its way.
        // Here we just return a completed nothing, but developers could listen for another message if they wanted.
        return CompletableFuture.completedFuture(null);
    });
    // Let PostgreSQL know we're listening
    return c.simpleQueryExec("LISTEN my_notifications").
        // Wait for 30 seconds for a single message.
        // To listen for more messages, we'd have to call this again.
        thenCompose(__ -> c.unsolicitedMessageTick(30, TimeUnit.SECONDS));
});

// Send a notification
pool.withConnection(c -> c.simpleQueryExec("NOTIFY my_notifications, 'test1'")).get();

// Wait for listener to end
listener.get();
```

In addition to notifications, developers can also listen for notices and server parameter/option changes (e.g. time zone
change). Note, when a connection is returned to a pool, all of its subscriptions are cleared. Same thing when a
connection is terminated. Therefore, developers who want to listen to notifications for a longer period should consider
creating a longer lived connection or just never giving the connection back to the pool.

#### Copy to a table

PostgreSQL supports a fast insert mode called a [COPY](https://www.postgresql.org/docs/current/static/sql-copy.html) and
PgNio supports it. Here's how to insert some CSV values:

```java
pool.withConnection(c ->
    c.simpleQueryExec("CREATE TEMP TABLE foo (bar VARCHAR(100), baz integer)").
        // Begin copy
        thenCompose(__ -> c.simpleCopyIn("COPY foo FROM STDIN CSV")).
        thenCompose(copy -> copy.sendData("test1,123\n".getBytes(StandardCharsets.UTF_8))).
        thenCompose(copy -> copy.sendData("test2,456\n".getBytes(StandardCharsets.UTF_8))).
        thenCompose(copy -> copy.done()).
        // Count should be 2
        thenCompose(__ -> c.simpleQueryRows("SELECT COUNT(1) FROM foo")).
        thenAccept(rows ->
            System.out.println("Count: " + RowReader.DEFAULT.get(rows.get(0), 0, Long.class)))
).get();
```

There are other formats including the default text format. `ParamWriter` can be used to help with this.

#### Copy from a table

Copying can also occur when reading out from a table:

```java
pool.withConnection(c ->
    c.simpleQueryExec("CREATE TEMP TABLE foo (bar VARCHAR(100), baz integer);" +
            "INSERT INTO foo VALUES ('test1', 123), ('test2', 456)").
        thenCompose(__ -> c.simpleCopyOut("COPY foo TO STDOUT CSV")).
        thenCompose(copy -> {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            return copy.
                receiveEachData(b -> {
                    try { bytes.write(b); }
                    catch (IOException e) { throw new RuntimeException(e); }
                }).
                thenAccept(__ ->
                    System.out.println("Got:\n" + new String(bytes.toByteArray(), StandardCharsets.UTF_8))).
                thenCompose(__ -> copy.done());
        })
).get();
```

#### Cancelling a query

In PostgreSQL, a long-running query cannot simply be cancelled within the same connection. Instead, a separate
connection must be created solely to cancel using the original connection's process ID and secret key:

```java
// We'll just set the process ID and secret key into an int array
CompletableFuture<int[]> processIdAndSecretKey = new CompletableFuture<>();

// Run a query for 10 seconds
CompletableFuture longQuery = pool.withConnection(c -> {
    // Set the process ID and secret key of this connection
    processIdAndSecretKey.complete(new int[] { c.getProcessId(), c.getSecretKey() });
    // Wait 10 seconds
    return c.simpleQueryExec("SELECT pg_sleep(10)");
});

// Kill that query
processIdAndSecretKey.thenCompose(idAndKey ->
    Connection.init(conf).thenCompose(c -> c.cancelOther(idAndKey[0], idAndKey[1]))).get();

// This will throw an exception since it was cancelled
longQuery.get();
```

Note, the newly created connection doesn't have to be explicitly closed/terminated because it is implied with
`cancelOther`.

#### More...

Many more cases are not covered here but can be learned from the code or test cases including:

* Advanced handling of query results including asking for one row at a time, skipping results, etc
* Fetching a maximum bound-query row set then fetching more
* Nested transactions
* Fetching results from multiple queries
* Using `flush` instead of `done` on prepared/bound queries
* Describing prepared statements to get parameter requirements
* Custom `Converters` for `RowReader` and/or `ParamWriter`
* `Notice` use and subscription
* `SSL` including use of custom `SSLContext`s to validate keys

### Data types

Below is a table of PostgreSQL types and their suggested Java data type. Some Java types can be used for multiple
PostgreSQL types and some PostgreSQL types can be represented by multiple Java types. These are listed in the order
they appear in the [PostgreSQL data type documentation](https://www.postgresql.org/docs/current/static/datatype.html)

| PostgreSQL Type | Java Type
| --- | --- |
| `smallint` | `java.lang.Short` |
| `integer` | `java.lang.Integer` |
| `bigint` | `java.lang.Long` |
| `decimal` | `java.lang.BigDecimal`<sup>1</sup> |
| `numeric` | `java.lang.BigDecimal`<sup>1</sup> |
| `real` | `java.lang.Float` |
| `double precision` | `java.lang.Double` |
| `smallserial` | `java.lang.Short` |
| `serial` | `java.lang.Integer` |
| `bigserial` | `java.lang.Long` |
| `money` | `pgnio.DataType.Money` |
| `varchar(n)` | `java.lang.String` |
| `char(n)` | `java.lang.String` |
| `text` | `java.lang.String` |
| `bytea` | `byte[]` |
| `timestamp without time zone` | `java.time.LocalDateTime` |
| `timestamp with time zone` | `java.time.OffsetDateTime` |
| `date` | `java.time.LocalDate` |
| `time without time zone` | `java.time.LocalTime` |
| `time with time zone` | `java.time.OffsetTime` |
| `interval` | `pgnio.DataType.Interval` |
| `boolean` | `java.lang.Boolean` |
| enumerated types | `java.lang.String` |
| `point` | `pgnio.DataType.Point` |
| `line` | `pgnio.DataType.Line` |
| `lseg` | `pgnio.DataType.LineSegment` |
| `box` | `pgnio.DataType.Box` |
| `path` | `pgnio.DataType.Path` |
| `polygon` | `pgnio.DataType.Polygon` |
| `circle` | `pgnio.DataType.Circle` |
| `inet` | `pgnio.DataType.Inet` |
| `cidr` | `pgnio.DataType.Inet` |
| `macaddr` | `pgnio.DataType.MacAddr` |
| `macaddr8` | `pgnio.DataType.MacAddr` |
| `bit(n)` | `java.lang.String` |
| `bit varying(n)` | `java.lang.String` |
| `tsvector` | `java.lang.String` |
| `tsquery` | `java.lang.String` |
| `uuid` | `java.util.UUID` |
| `xml` | `java.lang.String` |
| `json` | `java.lang.String` |
| `jsonb` | `java.lang.String` |
| arrays | arrays |
| `hstore` | `java.util.Map<String, String>` |
| all other types | `java.lang.String` |

Notes:

1. If `decimal` or `numeric` are expected to ever be NaN or infinity, users might prefer to deserialize to `String`
   first before converting to `BigDecimal`. Otherwise an exception occurs. For parameters that need to be NaN or
   infinity, consider using a float or double.

### FAQ

#### Why was this built?

My company needs a non-blocking PostgreSQL Java driver that is simple and yet can be used for advanced items. The other
ones carry unnecessary dependencies, are opinionated on what they make visible, aren't very configurable with
serialization, don't allow flexible use of the protocol, don't support all PostgreSQL features, and/or are
unmaintained (I've opened issues or made PRs on some of them). Granted there is no guarantee that this one will be
maintained forever either.

As mentioned in the features/goals section, this library is simple, extensible, and both low-level + high-level.
Serialization concerns are separated from protocol use. I also wanted to build this in preparation for the upcoming
async JDBC API and to develop a deep understanding of the PostgreSQL protocol.

#### Is "asynchronous" or "non-blocking" really better?

No. Sometimes it is when you don't want to use a thread per connection though internally NIO leverages thread
groups/pools. Also, since PostgreSQL's protocol doesn't support multiplexing a single connection there is even less
benefit than there might be with other protocols. Having said that, rarely is it worse and this library could easily be
used in a higher-level, synchronous, blocking application or library.

#### Why aren't there built-in conversions for lists, sets, etc?

In order to make this library simple, only the practical converters are included. Those collections can easily be
derived from arrays and/or custom converters can easily be written to build them.

#### Why don't the conversions support `Type` lookups instead of `Class` lookups?

For the `RowReader`, the `get` accepts a `Class` instead of a `Type`. There was no need using the current converters
to support generic types, but this may change in the future.

#### Why don't the conversions look up by implemented interface instead of just superclass?

For the current set of converters, simply traversing the class hierarchy to find a suitable conversion was good enough.
If there is a need for a converter for an interface, this could be supported in the future.

#### Why can't reading an `hstore` into a `Map` use the key and value types?

This library only supports `hstore` converting to a `Map<String, String>`. One might assume that, like arrays, it should
allow map values of other types that recursively does conversions on them. But PostgreSQL doesn't tell you the value
types of `hstore`. It was decided to perform the simple conversion. There is a `RowReader.get` call that accepts a
string if the caller wants to convert further, but it was decided that this library would not do it for them.

#### What about binary formatted parameters and results?

PostgreSQL has two formats in the protocol for parameters and results: binary and text. Right now, PgNio only supports
the text format (the default). The text format sends everything as normal strings and is portable across PostgreSQL
versions. This is usually good enough for almost all purposes. However, as more use cases for binary formatting come
about, it very well might be implemented in the library. In the meantime, the library is built to be extensible enough
that `ParamWriter`s and `RowReader`s operate purely on bytes and anyone can write binary formatters. Also, all protocol
calls that support specifying text or binary format are exposed to let the caller choose if they want.

### Development

#### Style

PgNio gladly accepts pull requests. In general the style is two-space indent, 120-char line max, and try to be clean
with line wrapping ideally with punctuation at the end of the line instead of the start. Since this is also a library
that can be used as a basis for others, we prefer to set the visibility protected instead of private or package-private
for anything that could have any value to anyone. We prefer fields over getters, nested classes over a bunch of files, simpler code over longer code, and clarity over confusion.

The [checker framework](https://checkerframework.org/) is used mainly to check nullability. This is preferred over
runtime checks for this library. Sometimes the initialization constraints get in the way, so feel free to mark code
`@SuppressWarnings("initialization")`.

#### Building

The project can be built with Gradle. Unlike other projects, PgNio does not bundle a Gradle wrapper script with the
repository. Simply download Gradle to `some/path` and run:

    some/path/bin/gradle --no-daemon :driver:assemble

Granted `--no-daemon` is just a choice that some choose to not keep a running Java process in the background, but it
will be a slower build. Also, the [checker framework](https://checkerframework.org/)'s annotation processor slows down
compilation quite a bit.

#### Testing

The unit tests are more like integration tests in that they actually run a PostgreSQL instance as an
[embedded PostgreSQL server](https://github.com/yandex-qatools/postgresql-embedded/). It will automatically download
itself and create directories as needed in `~/.embedpostgresql`. To run all tests, simply:

    some/path/bin/gradle --no-daemon :driver:test

By default it chooses the latest PostgreSQL version configured in the library (`10.2` as of this writing). A different
version can be used by setting the version number that appears in the
[download link](https://www.enterprisedb.com/download-postgresql-binaries) as the system property
`pgnio.postgres.version`. It is usually just the version with `-1` appended. So to test against `9.6.7`:

    some/path/bin/gradle --no-daemon :driver:test -Dpgnio.postgres.version=9.6.7-1

Note, on Windows sometimes the process remains open or there are other oddities. Developers may have to kill the
processes themselves and/or make sure the data files at `~/.embedpostgresql/data` are actually deleted (that is
the `C:\Users\username\.embedpostgresql\data` directory).

#### Using Latest Master

For updates that may not have been released into a numbered version, developers can use
[JitPack](https://jitpack.io/#cretz/pgnio/master-SNAPSHOT). Essentially this means using the JitPack resolver in the
build tool, and setting a dependency on the group `com.github.cretz`, name `pgnio`, and version `master-SNAPSHOT`.

#### Java 9+

When using Java 9 or newer to compile, the checker framework
[cannot perform checks](https://github.com/typetools/checker-framework/issues/1224) so it is disabled. For this reason,
developers ar encouraged to use Java 8 when compiling the `driver` project.

#### ADBA Support

Asynchronous database access support (a.k.a. ADBA, JDBC-Next, async JDBC, java.sql2, etc) is currently in development
in the `adba` subproject which uses Java 9. This means that Java 9+ must be used to compile it which, as mentioned
above, disables checker framework checks.

ADBA support requires the ADBA source which is available from the
[OpenJDK sandbox](http://hg.openjdk.java.net/jdk/sandbox/file/9d3b0eb749a9/src/jdk.incubator.adba) as of this writing.
Developers have to compile it to use it; [here](https://gist.github.com/cretz/fb21718d2456fe5d581c9d536c011d99) is a
`build.gradle` script that will build the ADBA JAR when `assemble` is run. Once the JAR is available, the full path to
the JAR must be set as the `adba.jar.path` system property when running the `adba` build in this project. E.g.:

    some/path/bin/gradle --no-daemon :adba:assemble -Dadba.jar.path=/full/path/to/jdk.incubator.adba.jar

Or if you are using an IDE such as IntelliJ this can be set as a Gradle option in the settings.

### TODO

* Streaming/logical replication
* Support other authentication options