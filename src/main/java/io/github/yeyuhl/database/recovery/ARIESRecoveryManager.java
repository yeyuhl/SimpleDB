package io.github.yeyuhl.database.recovery;

import io.github.yeyuhl.database.Transaction;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.concurrency.DummyLockContext;
import io.github.yeyuhl.database.io.DiskSpaceManager;
import io.github.yeyuhl.database.memory.BufferManager;
import io.github.yeyuhl.database.memory.Page;
import io.github.yeyuhl.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * ARIES恢复算法的实现类
 *
 * @author yeyuhl
 * @since 2023/7/25
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // 使用给定事务编号创建用于恢复的新事务的方法（Function类接收Long类型参数，返回Transaction类型结果）。
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // 如果重启的重做阶段已结束，则为true，否则为false。用于防止在重启的重做阶段刷新DPT条目。
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * 初始化日志，仅在首次设置数据库时调用。应将主记录添加到日志中，并创建检查点。
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * 设置buffer/disk managers，这不是构造函数的一部分，因为buffer manager和recovery manager之间存在循环依赖关系
     * buffer manager必须与recovery manager对接以阻止驱逐页面，直到日志被刷新
     * 但此时recovery manager需要与buffer manager对接，以写入日志和重做更改
     *
     * @param diskSpaceManager disk space manager
     * @param bufferManager    buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * 启动新事务时调用，应将事务添加到事务表中。
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * 当事务即将开始提交时调用，应追加提交记录，刷新日志，并更新事务表和事务状态。
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // 更新事务状态
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        // 追加提交记录
        transactionTable.get(transNum).lastLSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        long newLSN = transactionTable.get(transNum).lastLSN;
        // 刷新日志
        logManager.flushToLSN(newLSN);
        return newLSN;
    }

    /**
     * 当事务即将设置为中止时调用，应追加中止记录，并更新事务表和事务状态。
     * 调用此方法不应执行任何回滚操作。
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // 更新事务状态
        transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        // 追加中止记录
        transactionTable.get(transNum).lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, transactionTable.get(transNum).lastLSN));
        return transactionTable.get(transNum).lastLSN;
    }

    /**
     * 在事务清理时调用，如果事务中止，应回滚更改（请参阅下面的rollbackToLSN）。
     * 任何需要撤销的更改都应撤销，事务应从事务表中删除，并添加结束记录，并更新事务状态。
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        Transaction transaction = transactionTable.get(transNum).transaction;
        // 中止事务应该回滚更改
        if (transaction.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0);
        }
        // 由于进行了撤销，因此需要获取该事务的lastLSN
        long prevLSN = transactionTable.get(transNum).lastLSN;
        // 添加结束记录
        long newLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, prevLSN));
        transactionTable.get(transNum).lastLSN = newLSN;
        transactionTable.remove(transNum);
        transaction.setStatus(Transaction.Status.COMPLETE);
        return newLSN;
    }

    /**
     * Recommended helper function:
     * 执行事务所有操作的回滚，直至（但不包括）某个LSN。从尚未撤销的最新记录的LSN开始：
     * - 当当前的LSN大于我们要回滚到的LSN时：
     * - 如果当前LSN处的记录是可撤销的:
     * - 通过在记录上调用undo来获取补偿日志记录(CLR)
     * - Emit(发出) the CLR
     * - 调用CLR上的redo来执行撤销操作
     * - 将当前 LSN 更新为下一条要撤销记录的 LSN
     *
     * 请注意，在记录上调用.undo()并不执行撤销操作，它只是创建补偿日志记录(CLR)。
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN      LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // 小优化：如果最后一条记录是 CLR，我们可以从尚未撤销的下一条记录开始回滚。
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // 当当前的LSN大于我们要回滚到的LSN时：
        while (currentLSN > LSN) {
            LogRecord currentRecord = logManager.fetchLogRecord(currentLSN);
            if (currentRecord.isUndoable()) {
                // 通过在记录上调用undo来获取补偿日志记录(CLR)
                LogRecord CLR = currentRecord.undo(transactionEntry.lastLSN);
                transactionEntry.lastLSN = logManager.appendToLog(CLR);
                // 调用CLR上的redo来执行撤销操作
                CLR.redo(this, diskSpaceManager, bufferManager);
                // 将当前 LSN 更新为下一条要撤销记录的 LSN
                currentLSN = CLR.getUndoNextLSN().orElse(LSN);
            } else {
                currentLSN = currentRecord.getPrevLSN().orElse(LSN);
            }
        }
    }

    /**
     * 在从缓冲区缓存中刷新页面之前调用，日志页永远不会调用此方法。
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * 在磁盘上更新页面时调用，由于该页面不再是脏页，因此应从脏页表中删除。
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * 在写入页面时调用。
     *
     * 从不在日志页上调用此方法，前后参数的参数长度保证相同。
     *
     * 应添加相应的日志记录，并相应更新事务表和脏页表。
     *
     * @param transNum   transaction performing the write
     * @param pageNum    page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before     bytes starting at pageOffset before the write
     * @param after      bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before, byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        TransactionTableEntry ttEntry = transactionTable.get(transNum);
        // 创建更新日志记录并写入到日志
        LogRecord r = new UpdatePageLogRecord(transNum, pageNum, ttEntry.lastLSN, pageOffset, before, after);
        ttEntry.lastLSN = logManager.appendToLog(r);
        // 更新脏页表
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, ttEntry.lastLSN);
        }
        return ttEntry.lastLSN;
    }

    /**
     * 分配新分区时调用，日志刷新是必要的，因为返回后磁盘上会立即显示更改。
     * 如果分区是日志分区(分区号为0)，该方法应返回-1。应添加适当的日志记录，并刷新日志，事务表也应相应更新。
     *
     * @param transNum transaction requesting the allocation
     * @param partNum  partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 释放分区时调用，日志刷新是必要的，因为返回后磁盘上会立即显示更改。
     * 如果分区是日志分区，该方法应返回-1。应添加适当的日志记录，并刷新日志，事务表也应相应更新。
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum  partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 分配新页面时调用，日志刷新是必要的，因为返回后磁盘上会立即显示更改。
     * 如果分区是日志分区，该方法应返回-1。应添加适当的日志记录，并刷新日志，事务表也应相应更新。
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum  page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 释放页面时调用，日志刷新是必要的，因为返回后磁盘上会立即显示更改。
     * 如果分区是日志分区，该方法应返回-1。应添加适当的日志记录，并刷新日志，事务表也应相应更新。
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum  page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 为一个事务创建一个保存点，为事务创建与现有保存点同名的保存点时，应删除旧的保存点。
     * 应记录适当的LSN，以便日后进行部分回滚。
     *
     * @param transNum transaction to make savepoint for
     * @param name     name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * 释放（删除）一个事务的保存点。
     *
     * @param transNum transaction to delete savepoint for
     * @param name     name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * 将事务回滚到保存点。
     *
     * 事务自保存点以来所做的所有更改都应按相反顺序撤销，并将相应的CLR写入日志。事务状态应保持不变。
     *
     * @param transNum transaction to partially rollback
     * @param name     name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // 在LSN记录之后，所有严格意义上的事务变更都应撤销。
        long savepointLSN = transactionEntry.getSavepoint(name);
        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * 设置一个checkpoint。
     * 首先，应写入开始检查点记录。然后，应使用DPT中的recLSN尽可能填满结束检查点记录，使用事务表中的status/lastLSN填满记录，并在记录填满后（或无记录可写入时）写入结束检查点记录。
     * 你可以在这里找到EndCheckpointLogRecord#fitsInOneRecord方法，用于来确定何时写入结束检查点记录。
     * 最后，应使用开始检查点记录的LSN重写主记录。
     */
    @Override
    public synchronized void checkpoint() {
        // 创建开始检查点日志记录并写入日志
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        // 脏页到相应LSN的映射
        Map<Long, Long> chkptDPT = new HashMap<>();
        // 事务编号到相应事务状态和LSN的映射
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        int numDPTRecords = 0;
        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        // 遍历脏页表，获取脏页的page number
        for (Long pg : dirtyPageTable.keySet()) {
            numDPTRecords++;
            // 查看是否可以将所有结束检查点记录放入一个页面
            if (!EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords, 0)) {
                // 如果不可以，则需要拆分为多个EndCheckpointLogRecord存储
                numDPTRecords = 1;
                logManager.appendToLog(new EndCheckpointLogRecord(chkptDPT, chkptTxnTable));
                // 写入后清空检查点DPT
                chkptDPT.clear();
            }
            // 记录DPT
            chkptDPT.put(pg, dirtyPageTable.get(pg));
        }
        int numTXNTableRecords = 0;
        // 遍历事务表，获取事务编号
        for (Long tNum : transactionTable.keySet()) {
            numTXNTableRecords++;
            // 查看是否可以将所有结束检查点记录放入一个页面
            if (!EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords, numTXNTableRecords)) {
                // 同理，如果前面脏页相关的日志记录可以放到一个页面，但是考虑事务表相关的日志记录后不可以，那么也要拆分
                numDPTRecords = 0;
                numTXNTableRecords = 1;
                logManager.appendToLog(new EndCheckpointLogRecord(chkptDPT, chkptTxnTable));
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            // 记录事务表
            chkptTxnTable.put(tNum, new Pair<>(transactionTable.get(tNum).transaction.getStatus(),
                    transactionTable.get(tNum).lastLSN));
        }

        // 最后创建结束检查点日志记录并写入日志
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // 确保在更新主记录之前完全刷新检查点
        flushToLSN(endRecord.getLSN());

        // 更新主记录
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * 至少将日志刷新到指定记录，即刷新到包含LSN指定记录的页面。
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN, v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * 每当数据库启动时调用，并执行重启恢复。当返回的Runnable运行到终止时，恢复完成。该方法返回后，新事务即可启动。
     * 该方法应执行恢复的三个阶段，并在重做和撤销之间清理脏页面表中的非脏页面（缓冲区管理器中的非脏页面），并在撤销后执行检查点。
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * 该方法执行重启恢复的analysis pass。
     *
     * 首先，应读取主记录（LSN 0）。主记录包含一条信息：上次成功检查点的LSN。
     * 然后，从上次成功检查点的开头开始扫描日志记录。
     *
     * 如果日志记录涉及事务操作（getTransNum存在）：
     * - 更新事务表
     *
     * 如果日志记录涉及页面（getPageNum存在），更新dpt：
     * - update/undoupdate页面会弄脏页面
     * - free/undoalloc页面总是将更改刷新到磁盘
     * - alloc/undofree页面不需要任何操作
     *
     * 如果日志记录与事务状态变化有关：
     * - 如果是END_TRANSACTION：清理事务（Transaction#cleanup），从事务表中删除，并添加到endedTransactions
     * - 将事务状态更新为COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - 更新事务表
     *
     * 如果日志记录是END_CHECKPOINT记录：
     * - 复制检查点DPT的所有条目（如果有现有条目，则替换现有条目）
     * - 跳过已经结束的事务的事务表条目
     * - 如果还没有则添加到事务表
     * - 更新lastLSN，使其成为现有条目（如果有）和检查点条目中的较大者
     * - 如果可以从表中的状态转换到检查点中的状态，则应更新事务表中的状态。例如，running -> aborting是可能的转换，但aborting -> running不是。
     * 处理完所有记录后，清理并结束处于COMMITING状态的事务，并将所有处于RUNNING状态的事务移至RECOVERY_ABORTING/emit中止记录。
     */
    void restartAnalysis() {
        // 读取主记录
        LogRecord record = logManager.fetchLogRecord(0L);
        // 类型检查
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // 获取起始checkpoint的LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // 已完成的事务集
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> i = logManager.scanFrom(LSN);
        while (i.hasNext()) {
            LogRecord next = i.next();
            LogType lType = next.getType();
            // 如果涉及事务操作
            if (next.getTransNum().isPresent()) {
                // 更新事务表
                transactionTable.putIfAbsent(next.getTransNum().get(), new TransactionTableEntry(newTransaction.apply(next.getTransNum().get())));
                transactionTable.get(next.getTransNum().get()).lastLSN = next.getLSN();
                // 如果涉及页面（getPageNum存在），更新dpt
                if (next.getPageNum().isPresent()) {
                    // update/undoupdate页面会弄脏页面
                    if (next.getType().equals(LogType.UPDATE_PAGE) || next.getType().equals(LogType.UNDO_UPDATE_PAGE)) {
                        dirtyPageTable.putIfAbsent(next.getPageNum().get(), next.getLSN());
                    }
                    // free/undoalloc页面总是将更改刷新到磁盘
                    else if (next.getType().equals(LogType.FREE_PAGE) || next.getType().equals(LogType.UNDO_ALLOC_PAGE)) {
                        dirtyPageTable.remove(next.getPageNum().get());
                    }
                    continue;
                }
            }

            // 如果日志记录与事务状态变化有关
            if (lType.equals(LogType.COMMIT_TRANSACTION)) {
                Long transNum = next.getTransNum().get();
                Transaction t = transactionTable.get(transNum).transaction;
                t.setStatus(Transaction.Status.COMMITTING);
            } else if (lType.equals(LogType.ABORT_TRANSACTION)) {
                Long transNum = next.getTransNum().get();
                Transaction t = transactionTable.get(transNum).transaction;
                t.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
            // 如果是END_TRANSACTION，注意清理事务，从事务表中删除，并添加到endedTransactions
            else if (lType.equals(LogType.END_TRANSACTION)) {
                Long transNum = next.getTransNum().get();
                Transaction t = transactionTable.get(transNum).transaction;
                t.cleanup();
                t.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(next.getTransNum().get());
                endedTransactions.add(next.getTransNum().get());
            }

            // 如果是END_CHECKPOINT
            if (next.getType().equals(LogType.END_CHECKPOINT)) {
                Map<Long, Long> chkptDPT = next.getDirtyPageTable();
                Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = next.getTransactionTable();
                // 复制检查点DPT的所有条目（如果有现有条目，则替换现有条目）
                for (Long pg : chkptDPT.keySet()) {
                    dirtyPageTable.put(pg, chkptDPT.get(pg));
                }
                // 考虑事务表中的事务
                for (Long tNum : chkptTxnTable.keySet()) {
                    // 如果不在endedTransactions中
                    if (!endedTransactions.contains(tNum)) {
                        // 如果该事务不在事务表中，添加到事务表
                        transactionTable.putIfAbsent(tNum, new TransactionTableEntry(newTransaction.apply(tNum)));
                        TransactionTableEntry tEntry = transactionTable.get(tNum);
                        // 更新lastLSN，使其成为现有条目（如果有）和检查点条目中的较大者
                        if (chkptTxnTable.get(tNum).getSecond() > tEntry.lastLSN) {
                            tEntry.lastLSN = chkptTxnTable.get(tNum).getSecond();
                        }
                        Transaction.Status currStatus = chkptTxnTable.get(tNum).getFirst();
                        // 如果可以从表中的状态转换到检查点中的状态，则应更新事务表中的状态
                        if (tEntry.transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                            if (currStatus.equals(Transaction.Status.ABORTING)) {
                                tEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                            } else {
                                tEntry.transaction.setStatus(currStatus);
                            }
                        }
                        if (currStatus.equals(Transaction.Status.COMPLETE)) {
                            tEntry.transaction.cleanup();
                            tEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                        }
                    }
                    // 如果是已经结束的事务，跳过
                    else {
                        continue;
                    }
                }

            }
        }
        // 处理完所有记录后，清理并结束处于COMMITING状态的事务，并将所有处于RUNNING状态的事务移至RECOVERY_ABORTING/emit中止记录
        for (Long tNum : transactionTable.keySet()) {
            if (transactionTable.get(tNum).transaction.getStatus().equals(Transaction.Status.RUNNING)) {
                abort(tNum);
                transactionTable.get(tNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            } else if (transactionTable.get(tNum).transaction.getStatus().equals(Transaction.Status.COMMITTING)) {
                end(tNum);
            }
        }
        return;
    }

    /**
     * 此方法执行重启恢复的redo pass。
     *
     * 首先，从脏页表中确定REDO的起点
     *
     * 然后，从起点开始扫描，如果记录是redoable并且：
     * - 关于分区（Alloc/Free/UndoAlloc/UndoFree..Part），总是redo
     * - 分配页面（AllocPage/UndoFreePage），总是redo
     * - 修改了脏页表中的页面（Update/UndoUpdate/Free/UndoAlloc....Page）且LSN >= recLSN，从磁盘获取该页面，检查pageLSN，并在需要时重做记录。
     */
    void restartRedo() {
        // TODO(proj5): implement
        // DPT为空不用redo
        if (dirtyPageTable.isEmpty()) {
            return;
        }
        // 从脏页表中确定REDO的起点
        Long LSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> i = logManager.scanFrom(LSN);
        while (i.hasNext()) {
            LogRecord next = i.next();
            LogType lType = next.getType();
            if (next.isRedoable()) {
                boolean isPart = lType.equals(LogType.ALLOC_PART) ||
                        lType.equals(LogType.UNDO_ALLOC_PART) ||
                        lType.equals(LogType.FREE_PART) ||
                        lType.equals(LogType.UNDO_FREE_PART) ||
                        lType.equals(LogType.ALLOC_PAGE) ||
                        lType.equals(LogType.UNDO_FREE_PAGE);
                boolean isModified = lType.equals(LogType.UPDATE_PAGE) ||
                        lType.equals(LogType.UNDO_UPDATE_PAGE) ||
                        lType.equals(LogType.ALLOC_PAGE) ||
                        lType.equals(LogType.FREE_PAGE);
                if (isPart) {
                    next.redo(this, diskSpaceManager, bufferManager);
                } else if (isModified) {
                    Long pgNum = next.getPageNum().get();
                    if (dirtyPageTable.containsKey(pgNum) || next.getLSN() >= dirtyPageTable.get(pgNum)) {
                        Page p = bufferManager.fetchPage(new DummyLockContext(), pgNum);
                        try {
                            if (p.getPageLSN() >= next.getLSN()) {
                                continue;
                            }
                        } finally {
                            p.unpin();
                        }
                        next.redo(this, diskSpaceManager, bufferManager);
                    } else {
                        continue;
                    }
                }
            }
        }
        return;
    }

    /**
     * 此方法执行重启恢复的undo pass。
     * 首先，创建一个按所有中止事务其lastLSN大小作为排序的优先级队列，然后始终处理优先级队列中最大的LSN，直到完成为止，
     * - 如果记录是undoable，撤销它，并发出适当的CLR。
     * - 如果记录的undoNextLSN可用，则用它来替换集合中的条目，如果不可用则用prevLSN; 并且如果新的LSN为0，则结束事务并将其从队列和事务表中删除。
     */
    void restartUndo() {
        // TODO(proj5): implement
        PriorityQueue<Pair<Long, LogRecord>> undo = new PriorityQueue<>(new PairFirstReverseComparator<>());
        Map<Long, Long> lastLSN = new HashMap<>();
        // 创建一个按所有中止事务其lastLSN大小作为排序的优先级队列
        for (Long transNum : transactionTable.keySet()) {
            LogRecord next = logManager.fetchLogRecord(transactionTable.get(transNum).lastLSN);
            undo.offer(new Pair(next.getLSN(), next));
            lastLSN.put(transNum, transactionTable.get(transNum).lastLSN);
        }
        // 始终处理优先级队列中最大的LSN，直到完成为止
        while (!undo.isEmpty()) {
            LogRecord lr = undo.poll().getSecond();
            // 如果记录是undoable，撤销它，并发出适当的CLR
            if (lr.isUndoable()) {
                LogRecord clr = lr.undo(transactionTable.get(lr.getTransNum().get()).lastLSN);
                transactionTable.get(lr.getTransNum().get()).lastLSN = logManager.appendToLog(clr);
                lastLSN.put(lr.getTransNum().get(), transactionTable.get(lr.getTransNum().get()).lastLSN);
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            // 如果记录的undoNextLSN可用，则用它来替换集合中的条目，如果不可用则用prevLSN，实在不行则为0
            Long undoNextLSN = lr.getUndoNextLSN().orElse(lr.getPrevLSN().orElse(0L));
            if (undoNextLSN != 0L) {
                undo.offer(new Pair(undoNextLSN, logManager.fetchLogRecord(undoNextLSN)));
            }
            // 如果新的LSN为0，则结束事务并将其从队列和事务表中删除。
            else {
                TransactionTableEntry t = transactionTable.get(lr.getTransNum().get());
                t.transaction.cleanup();
                t.transaction.setStatus(Transaction.Status.COMPLETE);
                logManager.appendToLog(new EndTransactionLogRecord(t.transaction.getTransNum(), lastLSN.get(t.transaction.getTransNum())));
                transactionTable.remove(t.transaction.getTransNum());
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

// Helpers /////////////////////////////////////////////////////////////////

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}