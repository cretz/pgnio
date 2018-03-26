package pgnio;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Base class for all messages that can occur in a {@link QueryResultConnection}. All of the objects here are
 * detached from their original connections and can be used independent of them.
 */
public abstract class QueryMessage {
  /**
   * The 0-based index of the query this message is sent for. It is a simple counter and some messages may not be
   * query-specific.
   */
  public final int queryIndex;

  protected QueryMessage(int queryIndex) { this.queryIndex = queryIndex; }

  protected boolean isQueryEndingMessage() { return false; }

  /**
   * Sent when {@link QueryBuildConnection.Bound#execute(int)} is used and it has finished reading the partial result
   */
  public static class PortalSuspended extends QueryMessage {
    public PortalSuspended(int queryIndex) { super(queryIndex); }

    @Override
    protected boolean isQueryEndingMessage() { return true; }
  }

  /** Send as the final statement after simple or prepared queries have all completed */
  public static class ReadyForQuery extends QueryMessage {
    public ReadyForQuery(int queryIndex) { super(queryIndex); }
  }

  /** Sent when a single query has completed (so will occur multiple times for multiple queries) */
  public static class Complete extends QueryMessage {
    /** Metadata that may have been sent earlier via {@link RowMeta} on this query. Can be null. */
    public final @Nullable RowMeta meta;
    /**
     * The tag describing the result. Use {@link #getQueryType()}, {@link #getInsertedOid()}, and {@link #getRowCount()}
     * for parsed forms of this tag.
     */
    public final String tag;

    // Lazily loaded
    protected volatile @Nullable QueryType parsedQueryType;
    // Lazily loaded, 0 if not an insert of a single value
    protected volatile @Nullable Long insertedOid;
    // Lazily loaded, -1 means unknown
    protected volatile @Nullable Long parsedRowCount;

    public Complete(int queryIndex, @Nullable RowMeta meta, String tag) {
      super(queryIndex);
      this.meta = meta;
      this.tag = tag;
    }

    /** Get the query type for this completion, or {@link QueryType#UNKNOWN} is unrecognized. */
    public synchronized QueryType getQueryType() {
      if (parsedQueryType == null) {
        parsedQueryType = QueryType.UNKNOWN;
        int spaceIndex = tag.indexOf(' ');
        try {
          parsedQueryType = QueryType.valueOf(spaceIndex == -1 ? tag : tag.substring(0, spaceIndex));
        } catch (IllegalArgumentException ignored) { }
      }
      return parsedQueryType;
    }

    /**
     * If {@link #getQueryType()} is {@link QueryType#INSERT} and {@link #getRowCount()} is 1, this is the OID that was
     * inserted. Otherwise, it is null.
     */
    public synchronized @Nullable Long getInsertedOid() {
      if (insertedOid == null) {
        insertedOid = 0L;
        if (tag.startsWith("INSERT ")) {
          try {
            insertedOid = Long.valueOf(tag.substring(7, tag.indexOf(' ', 7)));
          } catch (Exception ignored) { }
        }
      }
      return insertedOid == 0L ? null : insertedOid;
    }

    /** The number of rows that were returned or affected as part of this query */
    public synchronized @Nullable Long getRowCount() {
      if (parsedRowCount == null) {
        parsedRowCount = -1L;
        int lastSpaceIndex = tag.lastIndexOf(' ');
        if (lastSpaceIndex != -1 && tag.length() > lastSpaceIndex + 1 &&
            tag.charAt(lastSpaceIndex + 1) >= '0' && tag.charAt(lastSpaceIndex + 1) <= '9') {
          try {
            parsedRowCount = Long.valueOf(tag.substring(lastSpaceIndex + 1));
          } catch (NumberFormatException ignored) { }
        }
      }
      return parsedRowCount == -1L ? null : parsedRowCount;
    }

    @Override
    protected boolean isQueryEndingMessage() { return true; }

    /** Known query types for query complete messages */
    public enum QueryType { CREATE, INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY, UNKNOWN }
  }

  /** Sent when an empty query string is seen */
  public static class EmptyQuery extends QueryMessage {
    public EmptyQuery(int queryIndex) { super(queryIndex); }

    @Override
    protected boolean isQueryEndingMessage() { return true; }
  }

  /** Sent when a prepared statement parse has completed */
  public static class ParseComplete extends QueryMessage {
    public ParseComplete(int queryIndex) { super(queryIndex); }
  }

  /** Sent when a prepared statement's parameters have been bound */
  public static class BindComplete extends QueryMessage {
    public BindComplete(int queryIndex) { super(queryIndex); }
  }

  /** Sent when a prepared statement or a bound portal has been closed */
  public static class CloseComplete extends QueryMessage {
    public CloseComplete(int queryIndex) { super(queryIndex); }
  }

  /** Sent when describing a statement that returns no rows */
  public static class NoData extends QueryMessage {
    public NoData(int queryIndex) { super(queryIndex); }
  }

  /** Individual row data */
  public static class Row extends QueryMessage {
    /** The last seen row metadata for this query if any */
    public final @Nullable RowMeta meta;
    /** The array of byte arrays representing this row's data */
    public final byte[]@Nullable [] raw;

    public Row(int queryIndex, @Nullable RowMeta meta, byte[]@Nullable [] raw) {
      super(queryIndex);
      this.meta = meta;
      this.raw = raw;
    }
  }

  /** Row metadata sent as part of a simple query or a describe */
  public static class RowMeta extends QueryMessage {
    /** The columns in order that they will appear in the result */
    public final Column[] columns;
    /** The same columns as {@link #columns}, keyed by their all-lowercased column names */
    public final Map<String, Column> columnsByName;

    public RowMeta(int queryIndex, Column[] columns, Map<String, Column> columnsByName) {
      super(queryIndex);
      this.columns = columns;
      this.columnsByName = columnsByName;
    }

    /** Metadata for an individual result column */
    public static class Column {
      /** The index in the result of this column */
      public final int index;
      /** The name of the column */
      public final String name;
      /** The table's OID if known, or 0 */
      public final int tableOid;
      /** The attribute number of the column in the table if known, or 0 */
      public final short columnAttributeNumber;
      /** The OID of the column data type. See {@link DataType} for some known ones. */
      public final int dataTypeOid;
      /** The pg_type.typelen type length of the value (negative for variable-length types) */
      public final short dataTypeSize;
      /** The pg_attribute.atttypmod type modifier */
      public final int typeModifier;
      /** Whether the result is in text format. This is usually true. */
      public final boolean textFormat;
      /** The column this is a child of if {@link #child(int)} was used to create this, otherwise null */
      public final @Nullable Column arrayParent;

      public Column(int index, String name, int tableOid, short columnAttributeNumber, int dataTypeOid,
          short dataTypeSize, int typeModifier, boolean textFormat) {
        this(index, name, tableOid, columnAttributeNumber, dataTypeOid, dataTypeSize, typeModifier, textFormat, null);
      }

      protected Column(int index, String name, int tableOid, short columnAttributeNumber, int dataTypeOid,
          short dataTypeSize, int typeModifier, boolean textFormat, @Nullable Column arrayParent) {
        this.index = index;
        this.name = name;
        this.tableOid = tableOid;
        this.columnAttributeNumber = columnAttributeNumber;
        this.dataTypeOid = dataTypeOid;
        this.dataTypeSize = dataTypeSize;
        this.typeModifier = typeModifier;
        this.textFormat = textFormat;
        this.arrayParent = arrayParent;
      }

      protected Column child(int dataTypeOid) {
        return new Column(index, name, tableOid, columnAttributeNumber, dataTypeOid,
            dataTypeSize, typeModifier, textFormat, this);
      }
    }
  }

  /** Metadata for the parameters the query accepts if {@link QueryBuildConnection.Prepared#describe()} was called */
  public static class ParamMeta extends QueryMessage {
    /** Array of data type OIDs. See {@link DataType} for some known ones */
    public final int[] dataTypeOids;

    public ParamMeta(int index, int[] dataTypeOids) {
      super(index);
      this.dataTypeOids = dataTypeOids;
    }
  }

  /** Signal that a copy can begin */
  public static class CopyBegin extends QueryMessage {
    /** The direction the copy will occur */
    public final Direction direction;
    /** Whether the overall copy is text or not */
    public final boolean text;
    /** Whether each individual column is text or not. Will all be true if {@link #text} is true */
    public final boolean[] columnsText;

    public CopyBegin(int queryIndex, Direction direction, boolean text, boolean[] columnsText) {
      super(queryIndex);
      this.direction = direction;
      this.text = text;
      this.columnsText = columnsText;
    }

    /** The possible directions of a copy */
    public enum Direction { IN, OUT, BOTH }
  }

  /** Data from the server on a copy */
  public static class CopyData extends QueryMessage {
    /** The raw copied bytes */
    public final byte[] bytes;

    public CopyData(int queryIndex, byte[] bytes) {
      super(queryIndex);
      this.bytes = bytes;
    }
  }

  /** Signal that a server-side copy is done */
  public static class CopyDone extends QueryMessage {
    public CopyDone(int queryIndex) { super(queryIndex); }
  }
}
