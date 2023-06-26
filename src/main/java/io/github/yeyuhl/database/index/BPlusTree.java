package io.github.yeyuhl.database.index;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.concurrency.LockContext;
import io.github.yeyuhl.database.concurrency.LockType;
import io.github.yeyuhl.database.concurrency.LockUtil;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.io.DiskSpaceManager;
import io.github.yeyuhl.database.memory.BufferManager;
import io.github.yeyuhl.database.table.RecordId;

import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * 一棵持久化的B+树
 * BPlusTree tree = new BPlusTree(bufferManager, metadata, lockContext);
 * <p>
 * // 往树中插入一些值
 * tree.put(new IntDataBox(0), new RecordId(0, (short) 0));
 * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
 * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
 * <p>
 * // 从树中获取一些值
 * tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 * tree.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 * tree.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 * tree.get(new IntDataBox(3)); // Optional.empty();
 * <p>
 * // 遍历树中的record ids
 * tree.scanEqual(new IntDataBox(2));        // [(2, 2)]
 * tree.scanAll();                           // [(0, 0), (1, 1), (2, 2)]
 * tree.scanGreaterEqual(new IntDataBox(1)); // [(1, 1), (2, 2)]
 * <p>
 * // 删除树上的一些元素
 * tree.get(new IntDataBox(0)); // Optional.of(RecordId(0, 0))
 * tree.remove(new IntDataBox(0));
 * tree.get(new IntDataBox(0)); // Optional.empty()
 * <p>
 * // 加载一棵树 (与创建一棵新树相同)
 * BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, lockContext);
 * <p>
 * // 所有的值还在那里
 * fromDisk.get(new IntDataBox(0)); // Optional.empty()
 * fromDisk.get(new IntDataBox(1)); // Optional.of(RecordId(1, 1))
 * fromDisk.get(new IntDataBox(2)); // Optional.of(RecordId(2, 2))
 *
 * @author yeyuhl
 * @since 2023/6/22
 */
public class BPlusTree {
    /**
     * Buffer manager
     */
    private BufferManager bufferManager;
    /**
     * B+ tree metadata
     */
    private BPlusTreeMetadata metadata;
    /**
     * B+ tree根节点
     */
    private BPlusNode root;
    /**
     * B+ tree的lock context
     */
    private LockContext lockContext;

    // Constructors ////////////////////////////////////////////////////////////

    /**
     * 构建一棵带有metadata和lock context的新B+树。
     * metadata包含顺序，分区号，根页号和键的类型信息。如果指定的阶数（order）太大，以至于单个节点不能放在一个页面上，那么会抛出一个BPlusTree异常。
     * 想拥有最大限度的完整的B+树节点，那么使用BPlusTree.maxOrder函数来获得适当的order。
     * 还要在_metadata.indices表中写入一行，其中包含有关B+树的metadata：
     * - the name of the tree (table associated with it and column it indexes)
     * - the key schema of the tree,
     * - the order of the tree,
     * - the partition number of the tree,
     * - the page number of the root of the tree.
     * <p>
     * 在给定分区上分配的所有页,都是内部节点和叶节点的序列化。
     */
    public BPlusTree(BufferManager bufferManager, BPlusTreeMetadata metadata, LockContext lockContext) {
        // 禁止子锁，只给整棵树上锁
        lockContext.disableChildLocks();
        // 默认读取整棵树
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.S);

        // Sanity checks
        if (metadata.getOrder() < 0) {
            String msg = String.format(
                    "You cannot construct a B+ tree with negative order %d.",
                    metadata.getOrder());
            throw new BPlusTreeException(msg);
        }

        int maxOrder = BPlusTree.maxOrder(BufferManager.EFFECTIVE_PAGE_SIZE, metadata.getKeySchema());
        if (metadata.getOrder() > maxOrder) {
            String msg = String.format(
                    "You cannot construct a B+ tree with order %d greater than the " + "max order %d.",
                    metadata.getOrder(), maxOrder);
            throw new BPlusTreeException(msg);
        }

        this.bufferManager = bufferManager;
        this.lockContext = lockContext;
        this.metadata = metadata;

        if (this.metadata.getRootPageNum() != DiskSpaceManager.INVALID_PAGE_NUM) {
            this.root = BPlusNode.fromBytes(this.metadata, bufferManager, lockContext,
                    this.metadata.getRootPageNum());
        } else {
            // 创建根节点，需要独占整棵树的访问权限
            LockUtil.ensureSufficientLockHeld(lockContext, LockType.X);
            // 构造根节点
            List<DataBox> keys = new ArrayList<>();
            List<RecordId> rids = new ArrayList<>();
            Optional<Long> rightSibling = Optional.empty();
            this.updateRoot(new LeafNode(this.metadata, bufferManager, keys, rids, rightSibling, lockContext));
        }
    }


    // Core API ////////////////////////////////////////////////////////////////

    /**
     * 获取key对应的value
     * <p>
     * // 往树中插入单个值
     * DataBox key = new IntDataBox(42);
     * RecordId rid = new RecordId(0, (short) 0);
     * tree.put(key, rid);
     * <p>
     * // 获取插入的值并且尝试获取一个从未插入的值
     * tree.get(key);                 // Optional.of(rid)
     * tree.get(new IntDataBox(100)); // Optional.empty()
     */
    public Optional<RecordId> get(DataBox key) {
        typecheck(key);
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        return root.get(key).getKey(key);
    }

    /**
     * scanEqual(k)等效于get(k)，但它返回的是迭代器而不是Optional
     * 如果get(k)返回Optional.empty()，则scanEqual(k)返回一个空的迭代器
     * 如果get(k)返回Optional.of(rid)，则scanEqual(k)返回一个遍历rid的迭代器
     */
    public Iterator<RecordId> scanEqual(DataBox key) {
        typecheck(key);
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        Optional<RecordId> rid = get(key);
        if (rid.isPresent()) {
            ArrayList<RecordId> l = new ArrayList<>();
            l.add(rid.get());
            return l.iterator();
        } else {
            return Collections.emptyIterator();
        }
    }

    /**
     * 存储在B+树中的所有RecordIds按相应keys的升序顺序返回一个迭代器
     * <p>
     * // 构建新B+树并插入值
     * BPlusTree tree = new BPlusTree("t.txt", Type.intType(), 4);
     * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     * tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     * tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     * tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     * <p>
     * Iterator<RecordId> iter = tree.scanAll();
     * iter.next(); // RecordId(1, 1)
     * iter.next(); // RecordId(2, 2)
     * iter.next(); // RecordId(3, 3)
     * iter.next(); // RecordId(4, 4)
     * iter.next(); // RecordId(5, 5)
     * iter.next(); // NoSuchElementException
     * <p>
     * 迭代器必须以懒加载的方式扫描B+树的叶节点，而不是取巧在内存中实例化所有RecordId，然后返回一个迭代器
     */
    public Iterator<RecordId> scanAll() {
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        return new BPlusTreeIterator(root.getLeftmostLeaf());
    }

    /**
     * 存储在B+树中的所有大于等于给定key的RecordIds按相应keys的升序顺序返回一个迭代器
     * <p>
     * // 构建新B+树并插入值
     * tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
     * tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
     * tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
     * tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
     * tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
     * <p>
     * Iterator<RecordId> iter = tree.scanGreaterEqual(new IntDataBox(3));
     * iter.next(); // RecordId(3, 3)
     * iter.next(); // RecordId(4, 4)
     * iter.next(); // RecordId(5, 5)
     * iter.next(); // NoSuchElementException
     * <p>
     * 同样的，迭代器必须以懒加载的方式扫描B+树的叶节点
     */
    public Iterator<RecordId> scanGreaterEqual(DataBox key) {
        typecheck(key);
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        LeafNode startNode = root.get(key);
        return new BPlusTreeIterator(startNode, key);
    }

    /**
     * 如果node溢出，需要分裂节点，在put方法和bulkLoad方法中要用到
     *           newRoot
     *          /       \
     *         /         \
     *  originalRoot    child
     *
     * @param key   newRoot的key
     * @param child newRoot的右孩子
     */
    private void splitRoot(DataBox key, Long child) {
        List<DataBox> keys = new ArrayList<>();
        // 将split_key插入到newRoot中
        keys.add(key);
        List<Long> children = new ArrayList<>();
        // 左孩子为originalRoot
        children.add(root.getPage().getPageNum());
        // 右孩子为新分裂出去的节点
        children.add(child);
        BPlusNode newRoot = new InnerNode(metadata, bufferManager, keys, children, lockContext);
        updateRoot(newRoot);
    }

    /**
     * 将一个(key，rid)对插入到B+树中
     * 如果B+树中已存在该key，则不会将该pair插入并抛出异常
     * <p>
     * DataBox key = new IntDataBox(42);
     * RecordId rid = new RecordId(42, (short) 42);
     * tree.put(key, rid); // Success :)
     * tree.put(key, rid); // BPlusTreeException :(
     */
    public void put(DataBox key, RecordId rid) {
        typecheck(key);
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        Optional<Pair<DataBox, Long>> pair = root.put(key, rid);
        // 如果返回的不是Optional.empty()，说明根节点溢出了，需要分裂根节点
        if (pair.isPresent()) {
            splitRoot(pair.get().getFirst(), pair.get().getSecond());
        }
        return;
    }

    /**
     * 批量加载数据到B+树中，树应该为空且数据迭代器应该按照DataBox键字段的顺序排序并且不包含重复项（不对此进行错误检查）
     * <p>
     * 填充因子仅适用于叶节点；内部节点应该像put方法一样填满并分成两半
     * <p>
     * 如果树在批量加载时不为空，调用该方法时应抛出异常。如果数据不满足前提条件（包含重复项或未按顺序排列），则结果行为属于是未定义。
     * 未定义的行为意味着可以根据需要（或根本不处理）处理这些情况，并且不需要编写任何显式检查。
     * <p>
     */
    public void bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        // 先判断树是否为空
        if (scanAll().hasNext()) {
            throw new BPlusTreeException("The tree is not empty, can't bulk load");
        }
        // 批量加载
        while(data.hasNext()){
            Optional<Pair<DataBox, Long>> pair = root.bulkLoad(data, fillFactor);
            if (pair.isPresent()) {
                splitRoot(pair.get().getFirst(), pair.get().getSecond());
            }
        }
        return;
    }

    /**
     * 从B+树中删除一个(key,rid)对
     * <p>
     * DataBox key = new IntDataBox(42);
     * RecordId rid = new RecordId(42, (short) 42);
     * <p>
     * tree.put(key, rid);
     * tree.get(key); // Optional.of(rid)
     * tree.remove(key);
     * tree.get(key); // Optional.empty()
     */
    public void remove(DataBox key) {
        typecheck(key);
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        root.remove(key);
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * 返回该树的sexps表示
     */
    public String toSexp() {
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        return root.toSexp();
    }


    /**
     * 对于较大规模的B+树来说，debug是十分困难的，为了便于debug，可以将B+树转换为DOT文件（可视化）
     * 调用tree.toDot()并将其输出保存到tree.dot文件中，然后运行以下命令：
     * <p>
     * dot -T pdf tree.dot -o tree.pdf
     * <p>
     * 将其转换为tree的PDF文件
     */
    public String toDot() {
        LockUtil.ensureSufficientLockHeld(lockContext, LockType.NL);
        List<String> strings = new ArrayList<>();
        strings.add("digraph g {");
        strings.add("  node [shape=record, height=0.1];");
        strings.add(root.toDot());
        strings.add("}");
        return String.join("\n", strings);
    }

    /**
     * 将B+树的dot文件转换为PDF并存储在src目录中
     */
    public void toDotPDFFile(String filename) {
        String tree_string = toDot();

        // Writing to intermediate dot file
        try {
            java.io.File file = new java.io.File("tree.dot");
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(tree_string);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Running command to convert dot file to PDF
        try {
            Runtime.getRuntime().exec("dot -T pdf tree.dot -o " + filename).waitFor();
        } catch (IOException e) {
            e.printStackTrace();
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new BPlusTreeException(e.getMessage());
        }
    }

    public BPlusTreeMetadata getMetadata() {
        return this.metadata;
    }

    /**
     * 返回最大数字d，以便具有2d个entries的LeafNode和具有2d个keys的InnerNode序列化后能将适应单个page
     */
    public static int maxOrder(short pageSize, Type keySchema) {
        int leafOrder = LeafNode.maxOrder(pageSize, keySchema);
        int innerOrder = InnerNode.maxOrder(pageSize, keySchema);
        return Math.min(leafOrder, innerOrder);
    }

    /**
     * 返回B+树所在的分区号
     */
    public int getPartNum() {
        return metadata.getPartNum();
    }

    /**
     * 保存新的root的page number并更新树的元数据
     */
    private void updateRoot(BPlusNode newRoot) {
        this.root = newRoot;
        metadata.setRootPageNum(this.root.getPage().getPageNum());
        metadata.incrementHeight();
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction != null) {
            transaction.updateIndexMetadata(metadata);
        }
    }

    private void typecheck(DataBox key) {
        Type t = metadata.getKeySchema();
        if (!key.type().equals(t)) {
            String msg = String.format("DataBox %s is not of type %s", key, t);
            throw new IllegalArgumentException(msg);
        }
    }

    // Iterator ////////////////////////////////////////////////////////////////

    private class BPlusTreeIterator implements Iterator<RecordId> {
        LeafNode currNode;
        Iterator<RecordId> currIter;

        public BPlusTreeIterator(LeafNode leafNode) {
            this.currNode = leafNode;
            this.currIter = leafNode.scanAll();
        }

        public BPlusTreeIterator(LeafNode leafNode, DataBox key) {
            this.currNode = leafNode;
            this.currIter = leafNode.scanGreaterEqual(key);
        }

        @Override
        public boolean hasNext() {
            if (currIter.hasNext()) {
                return true;
            } else {
                // 如果当前节点没有下一个元素，就找到下一个叶子节点
                Optional<LeafNode> nextNode = currNode.getRightSibling();
                if (nextNode.isPresent()) {
                    currNode = nextNode.get();
                    currIter = currNode.scanAll();
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public RecordId next() {
            if (currIter.hasNext()) {
                return currIter.next();
            } else {
                throw new NoSuchElementException();
            }
        }
    }
}
