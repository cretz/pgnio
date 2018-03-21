# AsyncPG

AsyncPG is an asynchronous PostgreSQL client for Java and the JVM. It was built to solve both simple and advanced
Postgres needs without being too opinionated or inflexible. Since all protocol features are supported callers can take
advanced approaches to problems.

See the latest [Javadoc](https://jitpack.io/com/github/cretz/asyncpg/driver/master-SNAPSHOT/javadoc/) for the driver on
the master branch (note, lazily generated so may be slow and/or need refresh on initial access). It excludes all
protected items since they are numerous and for extensibility only. TODO: show maven central javadocs instead once built

### Features/Goals

* Java 8+
* Simple and small codebase, no dependencies
* NIO2 and SSL
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

TODO: bintray/jcenter or maven central or jitpack once decided

### How To

Below are simple examples on how to use the client library. The library makes heavy use of composition with
`CompletableFuture` values which is why some of the code appears quite functional and non-ergonomic. All top-level
classes in the library are in the `asyncpg` package.

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

TODO

#### Execute simple queries

TODO

#### Reading row values

TODO

#### Prepare, bind, and execute queries with parameters

TODO

#### Use transactions

TODO

#### Reuse prepared queries

TODO

#### Listen for notifications

TODO

#### Copy to a table

TODO

#### Copy from a table

TODO

#### Cancelling a query

TODO

#### More...

Many more cases are not covered here but can be learned from the code or test cases including:

* Fetching a maximum bound-query row set then fetching more
* Nested transactions
* Fetching results from multiple queries
* Using `flush` instead of `done` on prepared/bound queries
* Describing prepared statements to get parameter requirements
* Custom `Converters` for `RowReader` and/or `ParamWriter`
* `Notice` use and subscription
* `SSL` including use of custom `SSLContext`s to validate keys

### Notes

#### Data types

TODO

#### Reading multidimensional array results

TODO

#### Testing on Windows

TODO

### FAQ

#### Why was this built?

TODO

#### Why aren't there built-in conversions for lists, sets, etc?

TODO

#### Why don't the conversions support `Type` lookups instead of `Class` lookups?

TODO

#### Why can't reading an `hstore` into a `Map` use the key and value types?

TODO

### TODO

* Streaming/logical replication
* Test different PG versions