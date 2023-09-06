package io.github.yeyuhl.database.query.join;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.iterator.BacktrackingIterator;
import io.github.yeyuhl.database.query.JoinOperator;
import io.github.yeyuhl.database.query.QueryOperator;
import io.github.yeyuhl.database.table.Record;


import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 使用BNLJ(块嵌套循环连接)算法分别在 leftColumnName 和 rightColumnName 上的两个关系之间执行等值连接
 *
 * @author yeyuhl
 * @since 2023/6/28
 */
public class BNLJOperator extends JoinOperator {
    /**
     * buffer pages的数量，Math.min(this.workMem, this.numMemoryPages)
     */
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction), leftColumnName, rightColumnName, transaction, JoinType.BNLJ);
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    /**
     * 估算块嵌套循环连接的IO成本
     */
    @Override
    public int estimateIOCost() {
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
                getLeftSource().estimateIOCost();
    }

    /**
     * 执行简单嵌套循环连接逻辑的record迭代器
     */
    private class BNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * 从左侧源获取下一个records块，leftBlockIterator应设置为回溯迭代器
         * 该迭代器最多包含来自左侧源的B-2页records，并且leftRecord应设置为此块中的第一条record
         *
         * 如果左侧源中没有更多的records，则此方法应不执行任何操作
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            // 没有更多的records，返回
            if (!leftSourceIterator.hasNext()) {
                return;
            }
            leftBlockIterator = QueryOperator.getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            leftBlockIterator.markNext();
            // 设置leftRecord为此块中的第一条record
            leftRecord = leftBlockIterator.next();
        }

        /**
         * 从正确的来源获取下一页records，rightPageIterator应设置为回溯迭代器，最多包含来自正确源的一页records
         *
         * 如果右侧源中没有更多的records，则此方法应不执行任何操作
         */
        private void fetchNextRightPage() {
            if (!rightSourceIterator.hasNext()) {
                return;
            }
            rightPageIterator = QueryOperator.getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            rightPageIterator.markNext();
        }

        /**
         * 返回应从此连接生成的下一条record(两个record连接形成的新record)，如果没有则返回null
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            Record rightRecord;
            while (true) {
                if (rightPageIterator.hasNext()) {
                }
                // 右侧page中的records都匹配过了，没有和leftRecord匹配上的，获取这个块中的下一条leftRecord
                // 注意要重置rightPageIterator，因为leftRecord变了，所以要重新匹配
                else if (leftBlockIterator.hasNext()) {
                    leftRecord = leftBlockIterator.next();
                    rightPageIterator.reset();
                }
                // 右侧page中的records都匹配过了，左侧块中的records没有一个匹配上的，获取下一个rightPage
                // 注意要重置leftBlockIterator，因为rightPage变了，所以要重新匹配
                else if (rightSourceIterator.hasNext()) {
                    leftBlockIterator.reset();
                    leftRecord = leftBlockIterator.next();
                    fetchNextRightPage();
                }
                // 所有右侧源中的records都匹配过了，左侧块中的records没有一个匹配上的，获取下一个leftBlock
                // 主要要重置rightSourceIterator，因为leftBlock变了，所以要重新匹配
                else if (leftSourceIterator.hasNext()) {
                    fetchNextLeftBlock();
                    rightSourceIterator.reset();
                    fetchNextRightPage();

                }
                // 全部匹配过了，还是匹配不上，返回null
                else {
                    return null;
                }
                // 从右侧page中获取下一个record
                rightRecord = rightPageIterator.next();
                // 如果左侧record和右侧record相等，则返回该连接形成的新record
                if (compare(leftRecord, rightRecord) == 0) {
                    return leftRecord.concat(rightRecord);
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
