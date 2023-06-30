package io.github.yeyuhl.database.query.join;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.HashFunc;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.common.iterator.BacktrackingIterator;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.query.JoinOperator;
import io.github.yeyuhl.database.query.QueryOperator;
import io.github.yeyuhl.database.query.disk.Partition;
import io.github.yeyuhl.database.query.disk.Run;
import io.github.yeyuhl.database.table.Record;
import io.github.yeyuhl.database.table.Schema;


import java.util.*;

/**
 * GHJO运算符类
 *
 * @author yeyuhl
 * @since 2023/6/29
 */
public class GHJOperator extends JoinOperator {
    private int numBuffers;
    private Run joinedRecords;

    public GHJOperator(QueryOperator leftSource,
                       QueryOperator rightSource,
                       String leftColumnName,
                       String rightColumnName,
                       TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.GHJ);
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
            // Executing GHJ on-the-fly is arduous without coroutines, so
            // instead we'll accumulate all of our joined records in this run
            // and return an iterator over it once the algorithm completes
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
     * 对于给定的迭代器中的每条record，对连接两张表的那一列的值进行哈希处理，并将其添加到正确的分区中
     *
     * @param partitions 分区数组
     * @param records    要进行分区的可迭代对象中的records
     * @param left       记录records是否来自左侧关系，如果是为true，否为false
     * @param pass       当前pass（用于选择一个hash函数）
     */
    private void partition(Partition[] partitions, Iterable<Record> records, boolean left, int pass) {
        for (Record record : records) {
            // 先判断左侧还是右侧，根据其index，获取对应的columnValue
            DataBox columnValue = left ? record.getValue(getLeftColumnIndex()) : record.getValue(getRightColumnIndex());
            // 对其进行hash处理
            int hash = HashFunc.hashDataBox(columnValue, pass);
            // 取模以获取要使用的分区
            int partitionNum = hash % partitions.length;
            // 哈希可能为负数，不进行处理存不到数组里面
            if (partitionNum < 0) {
                partitionNum += partitions.length;
            }
            partitions[partitionNum].add(record);
        }
    }

    /**
     * 在一个给定的分区上执行buildAndProbe，并将探测阶段发现的任何匹配的record添加到this.joinRecords
     */
    private void buildAndProbe(Partition leftPartition, Partition rightPartition) {
        // 如果探测records来自左侧分区，则为true，否则为false
        boolean probeFirst;
        // 使用这些records在内存中构建哈希表（外部表records）
        Iterable<Record> buildRecords;
        // 使用这些records探测table（内部表records）
        Iterable<Record> probeRecords;
        // build records的连接列的索引（连接列在外部表中的索引）
        int buildColumnIndex;
        // probe records的连接列的索引（连接列在内部表中的索引）
        int probeColumnIndex;

        if (leftPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = leftPartition;
            buildColumnIndex = getLeftColumnIndex();
            probeRecords = rightPartition;
            probeColumnIndex = getRightColumnIndex();
            probeFirst = false;
        } else if (rightPartition.getNumPages() <= this.numBuffers - 2) {
            buildRecords = rightPartition;
            buildColumnIndex = getRightColumnIndex();
            probeRecords = leftPartition;
            probeColumnIndex = getLeftColumnIndex();
            probeFirst = true;
        } else {
            throw new IllegalArgumentException(
                    "Neither the left nor the right records in this partition " + "fit in B-2 pages of memory."
            );
        }

        Map<DataBox, List<Record>> hashTable = new HashMap<>();
        // Building stage
        for (Record buildRecord : buildRecords) {
            DataBox buildJoinValue = buildRecord.getValue(buildColumnIndex);
            if (hashTable.containsKey(buildJoinValue)) {
                hashTable.get(buildJoinValue).add(buildRecord);
            } else {
                List<Record> list = new ArrayList<>();
                list.add(buildRecord);
                hashTable.put(buildJoinValue, list);
            }
        }
        // Probing stage
        for (Record probeRecord : probeRecords) {
            DataBox probeJoinValue = probeRecord.getValue(probeColumnIndex);
            if (!hashTable.containsKey(probeJoinValue)) {
                continue;
            }
            for (Record buildRecord : hashTable.get(probeJoinValue)) {
                // concat要判断左右
                Record joinedRecord = probeFirst ? probeRecord.concat(buildRecord) : buildRecord.concat(probeRecord);
                joinedRecords.add(joinedRecord);
            }
        }
    }

    /**
     * 运行GHJO算法，每一次传递都从划分leftRecords和rightRecords开始
     * 如果可以在一个分区上执行build和probe，则执行，否则应该递归地应用GHJO来进一步来划分分区
     */
    private void run(Iterable<Record> leftRecords, Iterable<Record> rightRecords, int pass) {
        assert pass >= 1;
        if (pass > 5) throw new IllegalStateException("Reached the max number of passes");

        // Create empty partitions
        Partition[] leftPartitions = createPartitions(true);
        Partition[] rightPartitions = createPartitions(false);

        // Partition records into left and right
        this.partition(leftPartitions, leftRecords, true, pass);
        this.partition(rightPartitions, rightRecords, false, pass);

        for (int i = 0; i < leftPartitions.length; i++) {
            // 如果可以在一个分区上执行build和probe，则执行
            if (leftPartitions[i].getNumPages() <= this.numBuffers - 2 || rightPartitions[i].getNumPages() <= this.numBuffers - 2) {
                buildAndProbe(leftPartitions[i], rightPartitions[i]);
            }
            // 不行的话，递归地应用GHJO来进一步来划分分区
            else {
                run(leftPartitions[i], rightPartitions[i], pass + 1);
            }
        }
    }

    // Provided Helpers ////////////////////////////////////////////////////////

    /**
     * Create an appropriate number of partitions relative to the number of
     * available buffers we have.
     *
     * @return an array of partitions
     */
    private Partition[] createPartitions(boolean left) {
        int usableBuffers = this.numBuffers - 1;
        Partition partitions[] = new Partition[usableBuffers];
        for (int i = 0; i < usableBuffers; i++) {
            partitions[i] = createPartition(left);
        }
        return partitions;
    }

    /**
     * Creates either a regular partition or a smart partition depending on the
     * value of this.useSmartPartition.
     *
     * @param left true if this partition will store records from the left
     *             relation, false otherwise
     * @return a partition to store records from the specified partition
     */
    private Partition createPartition(boolean left) {
        Schema schema = getRightSource().getSchema();
        if (left) schema = getLeftSource().getSchema();
        return new Partition(getTransaction(), schema);
    }

    // Student Input Methods ///////////////////////////////////////////////////

    /**
     * 使用int类型单列的value来创建一条record，额外的一列包含一个500字节的字符串，所以每页正好有8条records
     *
     * @param val value the field will take
     * @return a record
     */
    private static Record createRecord(int val) {
        String s = new String(new char[500]);
        return new Record(val, s);
    }

    /**
     * 该方法仅在test中被testBreakSHJButPassGHJ调用
     * <p>
     * 为leftRecords和rightRecords提供两个records列表，在这种情况下，SHJ会出错，但GHJ会成功运行
     * createRecord(int val)接收一个整数值，并返回a record with that value in the column being joined on
     * <p>
     * 两个连接都可以访问B=6的buffers，每个page恰好可以容纳8条records
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakSHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // SHJ breaks when trying to join them but not GHJ
        // B-1是分区数量上限，每个分区最大为B-2，所以最多可以有(B-1) * (B-2) * 8+1=161条records
        // So with (B-1) * (B-2) * 8 + 1 = 161 records, SHJ will fail
        // In fact, due to the key skew, with fewer records, SHJ may also fail
        for (int i = 0; i < 161; i++) {
            leftRecords.add(createRecord(i));
            rightRecords.add(createRecord(i));
        }
        return new Pair<>(leftRecords, rightRecords);
    }

    /**
     * 该方法仅在test中被GHJBreak调用
     * <p>
     * 为leftRecords和rightRecords提供两个records列表，在这种情况下，GHJ会出错，但SHJ会成功运行（在我们的例子中达到最大传递次数）
     * createRecord(int val)接收一个整数值，并返回a record with that value in the column being joined on
     * <p>
     * 两个连接都可以访问B=6的buffers，每个page恰好可以容纳8条records
     *
     * @return Pair of leftRecords and rightRecords
     */
    public static Pair<List<Record>, List<Record>> getBreakGHJInputs() {
        ArrayList<Record> leftRecords = new ArrayList<>();
        ArrayList<Record> rightRecords = new ArrayList<>();

        // since all the records are equal, there is only one partition after no matter how many passes
        // So with (B-2) * 8 + 1 = 33 records, GHJ will fail
        for (int i = 0; i < 33; i++) {
            leftRecords.add(createRecord(0));
            rightRecords.add(createRecord(0));
        }
        return new Pair<>(leftRecords, rightRecords);
    }
}

