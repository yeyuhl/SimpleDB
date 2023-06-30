package io.github.yeyuhl.database.query.join;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.HashFunc;
import io.github.yeyuhl.database.common.iterator.BacktrackingIterator;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.query.JoinOperator;
import io.github.yeyuhl.database.query.QueryOperator;
import io.github.yeyuhl.database.query.disk.Partition;
import io.github.yeyuhl.database.query.disk.Run;
import io.github.yeyuhl.database.table.Record;
import io.github.yeyuhl.database.table.Schema;


import java.util.*;

public class SHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    /**
     * 构建简单哈希连接运算符类
     * 为了连接这两个关系，SHJO将尝试对左侧records进行单个分区阶段，然后探测所有右侧records
     * 如果任何分区大于构建哈希表所需的B-2页内存，它将失败，抛出一个IllegalArgumentException。
     */
    public SHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SHJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
        this.joinedRecords = null;
    }

    @Override
    public int estimateIOCost() {
        // Since this has a chance of failing on certain inputs we give it the
        // maximum possible cost to encourage the optimizer to avoid it
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (joinedRecords == null) {
            // Accumulate all of our joined records in this run and return an
            // iterator over it once the algorithm completes
            this.joinedRecords = new Run(getTransaction(), getSchema());
            this.run(getLeftSource(), getRightSource(), 1);
        }
        ;
        return joinedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * 分区阶段，对于左侧记录迭代器中的每一条record，对要连接的列的值利用哈希函数处理，并将该record添加到正确的分区中
     */
    private void partition(Partition[] partitions, Iterable<Record> leftRecords) {
        for (Record record : leftRecords) {
            // 对所选列上的左侧records进行分区
            DataBox columnValue = record.getValue(getLeftColumnIndex());
            int hash = HashFunc.hashDataBox(columnValue, 1);
            // 取模以获取要使用的分区
            int partitionNum = hash % partitions.length;
            // 哈希可能为负数
            if (partitionNum < 0) {
                partitionNum += partitions.length;
            }
            partitions[partitionNum].add(record);
        }
    }

    /**
     * 使用leftRecords生成哈希表，并使用rightRecords中的records对其进行探测，连接匹配的records并将其作为joinRecords列表返回
     *
     * @param partition    a partition
     * @param rightRecords An iterable of records from the right relation
     */
    private void buildAndProbe(Partition partition, Iterable<Record> rightRecords) {
        if (partition.getNumPages() > this.numBuffers - 2) {
            throw new IllegalArgumentException("The records in this partition cannot fit in B-2 pages of memory.");
        }

        // 创建hash table，List<Record>包含了左边records中的所有记录，这些记录与同一个key进行哈希
        Map<DataBox, List<Record>> hashTable = new HashMap<>();

        // Building stage
        for (Record leftRecord : partition) {
            DataBox leftJoinValue = leftRecord.getValue(this.getLeftColumnIndex());
            if (!hashTable.containsKey(leftJoinValue)) {
                hashTable.put(leftJoinValue, new ArrayList<>());
            }
            hashTable.get(leftJoinValue).add(leftRecord);
        }

        // Probing stage
        for (Record rightRecord : rightRecords) {
            DataBox rightJoinValue = rightRecord.getValue(getRightColumnIndex());
            if (!hashTable.containsKey(rightJoinValue)) {
                continue;
            }
            // 将右边的record与每个左边的record用一个匹配的键连接起来
            for (Record lRecord : hashTable.get(rightJoinValue)) {
                Record joinedRecord = lRecord.concat(rightRecord);
                // 在this.joinRecords中加入的连接后的record
                this.joinedRecords.add(joinedRecord);
            }
        }
    }

    /**
     * 运行简单哈希连接算法
     * 首先，进入分区阶段以创建分区数组；然后，进入build和probe阶段，调用buildAndProbe方法
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] partitions = createPartitions();

        // Partition records into left and right
        this.partition(partitions, leftRecords);

        for (int i = 0; i < partitions.length; i++) {
            buildAndProbe(partitions[i], rightRecords);
        }
    }

    /**
     * 根据可用buffers的数量，创建一个适当数量的分区，并返回一个分区数组
     */
    private Partition[] createPartitions() {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            Schema schema = getLeftSource().getSchema();
            partitions[i] = new Partition(getTransaction(), schema);
        }
        return partitions;
    }
}

