package io.github.yeyuhl.database.index;

import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.databox.TypeId;
import io.github.yeyuhl.database.table.Record;

/**
 * B+树的元数据，包含各种信息
 *
 * @author yeyuhl
 * @since 2023/6/21
 */
public class BPlusTreeMetadata {
    /**
     * 该B+树隶属于哪个表
     */
    private final String tableName;

    /**
     * 该B+树使用哪一列作为搜索键
     */
    private final String colName;

    /**
     * B+树将键（某种类型）映射到记录id，记录键的类型。
     */
    private final Type keySchema;

    /**
     * B+树的阶数。给定阶数为d的树，其内部节点存储d到2d个键和d+1到2d+1个子节点指针。
     * 叶节点存储d到2d个（键，记录id）pairs。值得注意的是，包括根节点和已删除的叶节点；这些可能包含少于d个条目。
     */
    private final int order;

    /**
     * B+树分配页所使用的分区。B+树的每个节点都存储在此分区的不同页上。
     */
    private final int partNum;

    /**
     * 存储B+树的根节点的页号
     */
    private long rootPageNum;

    /**
     * 该B+树的高度
     */
    private int height;

    public BPlusTreeMetadata(String tableName, String colName, Type keySchema,
                             int order, int partNum, long rootPageNum, int height) {
        this.tableName = tableName;
        this.colName = colName;
        this.keySchema = keySchema;
        this.order = order;
        this.partNum = partNum;
        this.rootPageNum = rootPageNum;
        this.height = height;
    }

    public BPlusTreeMetadata(Record record) {
        this.tableName = record.getValue(0).getString();
        this.colName = record.getValue(1).getString();
        this.order = record.getValue(2).getInt();
        this.partNum = record.getValue(3).getInt();
        this.rootPageNum = record.getValue(4).getLong();
        this.height = record.getValue(7).getInt();
        int typeIdIndex = record.getValue(5).getInt();
        int typeSize = record.getValue(6).getInt();
        this.keySchema = new Type(TypeId.values()[typeIdIndex], typeSize);
    }

    /**
     * 将B+树的metadata转换为record，方便序列化
     */
    public Record toRecord() {
        return new Record(tableName, colName, order, partNum, rootPageNum,
                keySchema.getTypeId().ordinal(), keySchema.getSizeInBytes(),
                height
        );
    }

    public String getTableName() {
        return tableName;
    }

    public String getColName() {
        return colName;
    }

    public String getName() {
        return tableName + "," + colName;
    }

    public Type getKeySchema() {
        return keySchema;
    }

    public int getOrder() {
        return order;
    }

    public int getPartNum() {
        return partNum;
    }

    public long getRootPageNum() {
        return rootPageNum;
    }

    void setRootPageNum(long rootPageNum) {
        this.rootPageNum = rootPageNum;
    }

    public int getHeight() {
        return height;
    }

    void incrementHeight() {
        ++height;
    }
}
