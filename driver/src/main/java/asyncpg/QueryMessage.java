package asyncpg;

import java.util.Map;

public abstract class QueryMessage {
  public final int queryIndex;

  protected QueryMessage(int queryIndex) { this.queryIndex = queryIndex; }

  public static class Complete extends QueryMessage {
    public final RowMeta meta;
    public final QueryType type;
    // 0 if not an insert of a single value
    public final long insertedOid;
    public final long rowCount;

    public Complete(int queryIndex, RowMeta meta, QueryType type, long insertedOid, long rowCount) {
      super(queryIndex);
      this.meta = meta;
      this.type = type;
      this.insertedOid = insertedOid;
      this.rowCount = rowCount;
    }

    public enum QueryType { INSERT, DELETE, UPDATE, SELECT, MOVE, FETCH, COPY }
  }

  public static class EmptyQuery extends QueryMessage {
    protected EmptyQuery(int queryIndex) { super(queryIndex); }
  }

  public static class Row extends QueryMessage {
    // Can be null
    public final RowMeta meta;
    public final byte[][] raw;

    public Row(int queryIndex, RowMeta meta, byte[][] raw) {
      super(queryIndex);
      this.meta = meta;
      this.raw = raw;
    }
  }

  public static class RowMeta extends QueryMessage {
    public final Column[] columns;
    public final Map<String, Column> columnsByName;

    public RowMeta(int queryIndex, Column[] columns, Map<String, Column> columnsByName) {
      super(queryIndex);
      this.columns = columns;
      this.columnsByName = columnsByName;
    }

    public static class Column {
      public final String name;
      public final int tableOid;
      public final short columnAttributeNumber;
      public final int dataTypeOid;
      public final short dataTypeSize;
      public final int typeModifier;
      public final boolean formatText;

      public Column(String name, int tableOid, short columnAttributeNumber, int dataTypeOid, short dataTypeSize,
          int typeModifier, boolean formatText) {
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

  public static class BeginCopy extends QueryMessage {
    public final Direction direction;
    public final boolean text;
    public final boolean[] columnsText;

    public BeginCopy(int queryIndex, Direction direction, boolean text, boolean[] columnsText) {
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
}
