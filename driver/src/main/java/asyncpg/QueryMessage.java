package asyncpg;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public abstract class QueryMessage {
  public final int queryIndex;

  protected QueryMessage(int queryIndex) { this.queryIndex = queryIndex; }

  protected boolean isQueryEndingMessage() { return false; }
  protected boolean isQueryingDoneMessage() { return false; }

  public static class PortalSuspended extends QueryMessage {
    public PortalSuspended(int queryIndex) { super(queryIndex); }

    @Override
    protected boolean isQueryingDoneMessage() { return true; }
  }

  public static class ReadyForQuery extends QueryMessage {
    public ReadyForQuery(int queryIndex) { super(queryIndex); }

    @Override
    protected boolean isQueryingDoneMessage() { return true; }
  }

  public static class Complete extends QueryMessage {
    public final @Nullable RowMeta meta;
    public final QueryType type;
    // 0 if not an insert of a single value
    public final long insertedOid;
    public final long rowCount;

    public Complete(int queryIndex, @Nullable RowMeta meta, QueryType type, long insertedOid, long rowCount) {
      super(queryIndex);
      this.meta = meta;
      this.type = type;
      this.insertedOid = insertedOid;
      this.rowCount = rowCount;
    }

    @Override
    protected boolean isQueryEndingMessage() { return true; }

    public enum QueryType { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY }
  }

  public static class EmptyQuery extends QueryMessage {
    public EmptyQuery(int queryIndex) { super(queryIndex); }

    @Override
    protected boolean isQueryEndingMessage() { return true; }
  }

  public static class ParseComplete extends QueryMessage {
    public ParseComplete(int queryIndex) { super(queryIndex); }
  }

  public static class BindComplete extends QueryMessage {
    public BindComplete(int queryIndex) { super(queryIndex); }
  }

  public static class CloseComplete extends QueryMessage {
    public CloseComplete(int queryIndex) { super(queryIndex); }
  }

  public static class Row extends QueryMessage {
    public final @Nullable RowMeta meta;
    public final byte[]@Nullable [] raw;

    public Row(int queryIndex, @Nullable RowMeta meta, byte[]@Nullable [] raw) {
      super(queryIndex);
      this.meta = meta;
      this.raw = raw;
    }
  }

  public static class RowMeta extends QueryMessage {
    public final Column[] columns;
    // Keyed by lowercase column name
    public final Map<String, Column> columnsByName;

    public RowMeta(int queryIndex, Column[] columns, Map<String, Column> columnsByName) {
      super(queryIndex);
      this.columns = columns;
      this.columnsByName = columnsByName;
    }

    public static class Column {
      public final int index;
      public final String name;
      public final int tableOid;
      public final short columnAttributeNumber;
      public final int dataTypeOid;
      public final short dataTypeSize;
      public final int typeModifier;
      public final boolean formatText;

      public Column(int index, String name, int tableOid, short columnAttributeNumber, int dataTypeOid,
          short dataTypeSize, int typeModifier, boolean formatText) {
        this.index = index;
        this.name = name;
        this.tableOid = tableOid;
        this.columnAttributeNumber = columnAttributeNumber;
        this.dataTypeOid = dataTypeOid;
        this.dataTypeSize = dataTypeSize;
        this.typeModifier = typeModifier;
        this.formatText = formatText;
      }
    }
  }

  public static class ParamMeta extends QueryMessage {
    public final int[] dataTypeOids;

    public ParamMeta(int index, int[] dataTypeOids) {
      super(index);
      this.dataTypeOids = dataTypeOids;
    }
  }

  public static class CopyBegin extends QueryMessage {
    public final Direction direction;
    public final boolean text;
    public final boolean[] columnsText;

    public CopyBegin(int queryIndex, Direction direction, boolean text, boolean[] columnsText) {
      super(queryIndex);
      this.direction = direction;
      this.text = text;
      this.columnsText = columnsText;
    }

    public enum Direction { IN, OUT, BOTH }
  }

  public static class CopyData extends QueryMessage {
    public final byte[] bytes;

    public CopyData(int queryIndex, byte[] bytes) {
      super(queryIndex);
      this.bytes = bytes;
    }
  }

  public static class CopyDone extends QueryMessage {
    public CopyDone(int queryIndex) { super(queryIndex); }
  }
}
