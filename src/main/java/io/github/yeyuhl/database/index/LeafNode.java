package io.github.yeyuhl.database.index;

import com.sun.webkit.PageCache;
import io.github.yeyuhl.database.common.Buffer;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.concurrency.LockContext;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.databox.Type;
import io.github.yeyuhl.database.memory.BufferManager;
import io.github.yeyuhl.database.memory.Page;
import io.github.yeyuhl.database.table.RecordId;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * B+树的叶节点，d阶B+树中的每个叶节点都存储着d到2d(key, record id)个对和一个指向其右同级叶节点的指针（即其page number）
 * 此外，每个叶节点同样都是序列化并持久化在单个page上
 * 例如，下面是连接在一起的两个2阶叶节点的图示：
 * <p>
 * leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 * | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 *
 * @author yeyuhl
 * @since 2023/6/26
 */
class LeafNode extends BPlusNode {
    /**
     * 该叶节点所在的B+树的元数据
     */
    private BPlusTreeMetadata metadata;

    /**
     * Buffer manager
     */
    private BufferManager bufferManager;

    /**
     * 该B+ tree的Lock context
     */
    private LockContext treeContext;

    /**
     * 该叶节点序列化后所存储在的page
     */
    private Page page;

    /**
     * 该叶节点的keys和record ids
     * 其中“keys”始终按升序排序，在索引i处的record id对应于索引i处的键
     * 例如，keys [a， b， c] 和 rids [1， 2， 3] 表示配对 [a：1， b：2， c：3]
     * <p>
     * keys和rids是存储在磁盘上的keys和record ids在内存中的缓存，因此，要考虑在创建两个指向同一page的LeafNode对象时会发生什么情况：
     * BPlusTreeMetadata meta = ...;
     * int pageNum = ...;
     * LockContext treeContext = new DummyLockContext();
     * LeafNode leaf0 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
     * LeafNode leaf1 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
     * <p>
     * 此方案如下所示：
     * HEAP                        | DISK
     * ===============================================================
     * leaf0                       | page 42
     * +-------------------------+ | +-------+-------+-------+-------+
     * | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
     * | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * leaf1                       |
     * +-------------------------+ |
     * | keys = [k0, k1, k2]     | |
     * | rids = [r0, r1, r2]     | |
     * | pageNum = 42            | |
     * +-------------------------+ |
     * <p>
     * 现在想象一下，我们对leaf0执行操作，如leaf0.put(k3,r3)，leaf0的内存中值将更新，并将同步到磁盘
     * 但是，leaf1内存中的值将不会更新。这将看起来像这样：
     * <p>
     * HEAP                        | DISK
     * ===============================================================
     * leaf0                       | page 42
     * +-------------------------+ | +-------+-------+-------+-------+
     * | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
     * | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * leaf1                       |
     * +-------------------------+ |
     * | keys = [k0, k1, k2]     | |
     * | rids = [r0, r1, r2]     | |
     * | pageNum = 42            | |
     * +-------------------------+ |
     * <p>
     * 这就是缓存一致性问题，我们需要保证不会使用过时的内存中缓存的keys和rids的值
     */
    private List<DataBox> keys;
    private List<RecordId> rids;

    /**
     * 该叶节点的右同级叶节点
     * 如果此叶节点是最右边的叶节点，则右同级是Optional.empty()
     * 否则，右同级是Optional.of(n)，其中n是此叶节点的右同级的页码
     */
    private Optional<Long> rightSibling;

    // Constructors ////////////////////////////////////////////////////////////

    /**
     * 构造一个全新的叶节点，该构造方法将从提供的bufferManager中获取新的固定页，并将节点持久化到该页
     */
    LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
                keys, rids, rightSibling, treeContext);
    }

    /**
     * 构造一个持久化到指定page的叶节点
     */
    private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                     List<DataBox> keys, List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        try {
            assert (keys.size() == rids.size());
            assert (keys.size() <= 2 * metadata.getOrder());
            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.rids = new ArrayList<>(rids);
            this.rightSibling = rightSibling;
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////
    @Override
    public LeafNode get(DataBox key) {
        return this;
    }

    @Override
    public LeafNode getLeftmostLeaf() {
        return this;
    }

    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // 先判断是否存在相同的key
        if (keys.contains(key)) {
            throw new BPlusTreeException("insert duplicate entries with the same key");
        }
        // 找到key应该插入的位置
        int index = InnerNode.numLessThan(key, keys);
        keys.add(index, key);
        rids.add(index, rid);
        // 判断是否需要分裂
        if (keys.size() <= 2 * metadata.getOrder()) {
            // 没溢出，插入并返回Optional.empty()
            sync();
            return Optional.empty();
        } else {
            // 溢出，前d个entries留在当前节点，d+1及之后的entries放在新分裂出来的节点
            // 右同级叶节点需要的keys和rids
            List<DataBox> rightKeys = keys.subList(metadata.getOrder(), keys.size());
            List<RecordId> rightRids = rids.subList(metadata.getOrder(), rids.size());
            // 原节点需要的keys和rids
            keys = keys.subList(0, metadata.getOrder());
            rids = rids.subList(0, metadata.getOrder());
            // 创建新的叶节点
            LeafNode rightNode = new LeafNode(metadata, bufferManager, rightKeys, rightRids, rightSibling, treeContext);
            rightSibling = Optional.of(rightNode.getPage().getPageNum());
            sync();
            return Optional.of(new Pair<>(rightKeys.get(0), rightNode.getPage().getPageNum()));
        }
    }

    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // 叶节点的能存储的最大records数
        int maxRecords = (int) Math.floor(2 * metadata.getOrder() * fillFactor);
        while (keys.size() < maxRecords && data.hasNext()) {
            Pair<DataBox, RecordId> pair = data.next();
            keys.add(pair.getFirst());
            rids.add(pair.getSecond());
        }
        // 当叶节点已经存满了，需要分裂
        Optional<Pair<DataBox, Long>> result = Optional.empty();
        if (data.hasNext()) {
            List<DataBox> rightKeys = new ArrayList<>();
            List<RecordId> rightRids = new ArrayList<>();
            Pair<DataBox, RecordId> p = data.next();
            rightKeys.add(p.getFirst());
            rightRids.add(p.getSecond());
            LeafNode rightNode = new LeafNode(metadata, bufferManager, rightKeys, rightRids, rightSibling, treeContext);
            rightSibling = Optional.of(rightNode.getPage().getPageNum());
            result = Optional.of(new Pair(p.getFirst(), rightSibling.get()));
        }
        sync();
        return result;
    }


    @Override
    public void remove(DataBox key) {
        int index = keys.indexOf(key);
        if (index > 0) {
            keys.remove(index);
            rids.remove(index);
            sync();
        }
        return;
    }

    // Iterators ///////////////////////////////////////////////////////////////

    /**
     * 返回与key相对应的record id
     */
    Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * 存储在该叶节点的所有RecordIds按相应keys的升序顺序返回一个迭代器
     */
    Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    /**
     * 存储在该叶节点的所有大于等于给定key的RecordIds按相应keys的升序顺序返回一个迭代器
     */
    Iterator<RecordId> scanGreaterEqual(DataBox key) {
        int index = InnerNode.numLessThan(key, keys);
        return rids.subList(index, rids.size()).iterator();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /**
     * 如果有右同级叶节点，则返回右同级节点
     */
    Optional<LeafNode> getRightSibling() {
        if (!rightSibling.isPresent()) {
            return Optional.empty();
        }
        long pageNum = rightSibling.get();
        return Optional.of(LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
    }

    /**
     * 序列化叶节点并保存到对应的page上
     */
    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    /**
     * 测试用
     */
    List<DataBox> getKeys() {
        return keys;
    }

    /**
     * 测试用
     */
    List<RecordId> getRids() {
        return rids;
    }

    /**
     * 返回最大数字d，以便具有2d个条目的LeafNode序列化后能适应单个page大小
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // 具有n个条目的叶节点占用的字节数为:1 + 8 + 4 + n * (keySize + ridSize)
        //   - 1字节用于存储isLeaf
        //   - 8字节用于存储sibling指针，即指向右同级叶节点的指针
        //   - 4字节用于存储n
        //   - keySize表示用于存储keySchema类型的DataBox的字节数
        //   - ridSize表示用于存储RecordId的字节数
        //
        //   根据以上条件得到这个不等式：
        //   n * (keySize + ridSize) + 13 <= pageSizeInBytes
        //   求解后得到：
        //   n = (pageSizeInBytes - 13) / (keySize + ridSize)
        //
        // 其中阶数d = n / 2
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + ridSize);
        return n / 2;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        String rightSibString = rightSibling.map(Object::toString).orElse("None");
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s, rightSibling=%s)",
                page.getPageNum(), keys, rids, rightSibString);
    }

    @Override
    public String toSexp() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * 给定一个page number为1的叶节点，存储有三个(key, rid)pairs，(0, (0, 0))，(1, (1, 1))和(2, (2, 2))
     * 用dot表示为：
     * <p>
     * node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // 当序列化内部节点时，序列化后的内容为：
        //   a. 文本值为1(1字节)，表示此节点是叶节点
        //   b. 右同级叶节点的page number(8字节)，如果没有用-1表示
        //   c. 该叶节点包含的(key, rid)pairs的数量(4字节)
        //   d. (key, rid)pairs本身
        //
        // 举个例子：第4页上具有右同级节点的叶节点，有一个(3,(pageNum=3,entryNum=1))的pair
        //   +----+-------------------------+-------------+----+-------------------------------+
        //   | 01 | 00 00 00 00 00 00 00 04 | 00 00 00 01 | 03 | 00 00 00 00 00 00 00 03 00 01 |
        //   +----+-------------------------+-------------+----+-------------------------------+
        //    \__/ \_______________________/ \___________/ \__________________________________/
        //     a               b                   c                         d
        //
        assert (keys.size() == rids.size());
        assert (keys.size() <= 2 * metadata.getOrder());

        // All sizes are in bytes.
        int isLeafSize = 1;
        int siblingSize = Long.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.putLong(rightSibling.orElse(-1L));
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * 根据pageNum从指定page中加载页节点
     */
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager, LockContext treeContext, long pageNum) {
        // 实现fromBytes方法需要重用已有的页，而不是获取一个新的页，参考InnerNode.fromBytes方法
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buffer = page.getBuffer();
        byte nodeType = buffer.get();
        assert (nodeType == (byte) 1);
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        long rs = buffer.getLong();
        // 如果右同级叶节点的page number为-1，表示没有右同级叶节点，因此查看其是否有右同级叶节点
        Optional<Long> rightSibling = rs == -1 ? Optional.empty() : Optional.of(rs);
        // 获取该叶节点包含的(key, rid)pairs的数量
        int nums = buffer.getInt();
        for (int i = 0; i < nums; i++) {
            keys.add(DataBox.fromBytes(buffer, metadata.getKeySchema()));
            rids.add(RecordId.fromBytes(buffer));
        }
        return new LeafNode(metadata, bufferManager, page, keys, rids, rightSibling, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode)) {
            return false;
        }
        LeafNode n = (LeafNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                rids.equals(n.rids) &&
                rightSibling.equals(n.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}
