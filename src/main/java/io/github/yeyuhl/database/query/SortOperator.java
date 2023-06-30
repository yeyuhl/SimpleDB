package io.github.yeyuhl.database.query;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.common.iterator.BacktrackingIterator;
import io.github.yeyuhl.database.query.disk.Run;
import io.github.yeyuhl.database.table.Record;
import io.github.yeyuhl.database.table.Schema;
import io.github.yeyuhl.database.table.stats.TableStats;


import java.util.*;

/**
 * 排序运算符，按照指定列对记录进行排序
 */
public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double) numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * run本质上是一个迭代器，不过存储的是排好序的records，排序可以直接用Java自带的sort来实现
     *
     * @return 一个排序好的run，包含来自输入迭代器的所有records
     */
    public Run sortRun(Iterator<Record> records) {
        Run sortedRun = new Run(transaction, getSchema());
        List<Record> listRecords = new ArrayList<>();
        while (records.hasNext()) {
            listRecords.add(records.next());
        }
        listRecords.sort(new RecordComparator());
        sortedRun.addAll(listRecords);
        return sortedRun;
    }

    /**
     * 给定一个包含有序的runs的列表，返回一个新的run，该run是合并输入runs的结果
     * 应该使用一个优先队列（PriorityQueue）来决定哪个record应该被添加到下一个输出run中
     * <p>
     * 优先队列中不允许有超过runs.size()条records，建议优先队列中存储Pair<Record, Integer>对象
     * 其中Pair(r, i)表示是来自run i的Record r，且r是当前未合并的run中最小的值，i可用于定位下一个要添加到队列中的record
     *
     * @return 通过合并输入runs获得一个排好序的run
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        Run sortedRun = new Run(transaction, getSchema());
        List<BacktrackingIterator<Record>> iters = new ArrayList<>();
        for (Run run : runs) {
            iters.add(run.iterator());
        }
        PriorityQueue<Pair<Record, Integer>> priorityQueue = new PriorityQueue<>(runs.size(), new RecordPairComparator());
        for (int i = 0; i < iters.size(); i++) {
            // 保证每个run都有一个record被添加到优先队列中且是最小的
            if (iters.get(i).hasNext()) {
                priorityQueue.add(new Pair<>(iters.get(i).next(), i));
            }
        }
        while (!priorityQueue.isEmpty()) {
            // 每次从优先队列中取出最小的record，添加到sortedRun中
            Pair<Record, Integer> pair = priorityQueue.poll();
            Record r = pair.getFirst();
            int i = pair.getSecond();
            sortedRun.add(r);
            // 由于i的最小record从优先队列中移除了，因此如果run i还有record，将其添加到优先队列中
            if (iters.get(i).hasNext()) {
                priorityQueue.add(new Pair<>(iters.get(i).next(), i));
            } else {
                continue;
            }
        }
        return sortedRun;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * 给定一个包含N个有序的runs的列表，返回一个有序的runs的列表，即把列表里的runs合并了一部分
     * 该列表是通过每次合并(numBuffers - 1)个输入runs得到的，如果N不是(numBuffers - 1)的完美倍数，那么最后一个有序的run应该是合并少于(numBuffers - 1)个runs的得到的
     *
     * @return 通过合并输入runs获得一个包含有序的runs的列表
     */
    public List<Run> mergePass(List<Run> runs) {
        List<Run> mergedRuns = new ArrayList<>();
        int len = runs.size();
        int size = this.numBuffers - 1;
        // 每次合并(numBuffers - 1)个输入runs
        for (int i = 0; i + size <= len; i += size) {
            mergedRuns.add(mergeSortedRuns(runs.subList(i, Math.min(i + size, len))));
        }
        return mergedRuns;
    }

    /**
     * 外部合并排序算法的实现
     *
     * @return 一个包含源运算符所有records的有序的run
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        List<Run> sortedRuns = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            // getBlockIterator方法返回一个包含numBuffers个records的迭代器
            sortedRuns.add(sortRun(getBlockIterator(sourceIterator, getSchema(), numBuffers)));
        }
        // 通过mergePass方法合并runs，合并到最后只剩下一个run
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }
        return sortedRuns.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

