package io.github.yeyuhl.database.query.join;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.iterator.BacktrackingIterator;
import io.github.yeyuhl.database.query.JoinOperator;
import io.github.yeyuhl.database.query.QueryOperator;
import io.github.yeyuhl.database.table.Record;


import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 使用SNLJ(简单嵌套循环连接)算法分别在 leftColumnName 和 rightColumnName 上的两个关系之间执行等值连接
 */
public class SNLJOperator extends JoinOperator {
    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction), leftColumnName, rightColumnName, transaction, JoinType.SNLJ);
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        int numLeftRecords = getLeftSource().estimateStats().getNumRecords();
        int numRightPages = getRightSource().estimateStats().getNumPages();
        return numLeftRecords * numRightPages + getLeftSource().estimateIOCost();
    }

    /**
     * 执行SNLJ逻辑的record迭代器
     * 左表是“外部”循环(外部表)，右表是“内部”循环(内部表)
     */
    private class SNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left relation
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right relation
        private BacktrackingIterator<Record> rightSourceIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        public SNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            if (leftSourceIterator.hasNext()) leftRecord = leftSourceIterator.next();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
        }

        /**
         * 返回应从此连接生成的下一条record，如果没有则返回null
         */
        private Record fetchNextRecord() {
            if (leftRecord == null) {
                // The left source was empty, nothing to fetch
                return null;
            }
            while (true) {
                if (this.rightSourceIterator.hasNext()) {
                    // there's a next right record, join it if there's a match
                    Record rightRecord = rightSourceIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else if (leftSourceIterator.hasNext()) {
                    // there's no more right records but there's still left
                    // records. Advance left and reset right
                    this.leftRecord = leftSourceIterator.next();
                    this.rightSourceIterator.reset();
                } else {
                    // if you're here then there are no more records to fetch
                    return null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }

}

