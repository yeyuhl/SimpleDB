package io.github.yeyuhl.database.index;

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
 * B+树的内部节点，d阶B+树中的每个内部节点存储着d到2d个键
 * 具有n个键的内部节点存储指向子节点的n+1个“指针”（指针其实是一个page number），此外每个内部节点都序列化并持久化在单个page上
 * 例如，下面是2阶的内部节点的图示：
 *
 * +----+----+----+----+
 * | 10 | 20 | 30 |    |
 * +----+----+----+----+
 * /     |    |     \
 *
 * @author yeyuhl
 * @since 2023/6/26
 */
class InnerNode extends BPlusNode {
    /**
     * 该内部节点所在的B+树的元数据
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
     * 该内部节点序列化后所存储在的page
     */
    private Page page;

    /**
     * 该内部节点的键
     */
    private List<DataBox> keys;

    /**
     * 子节点指针，实际上是page number
     */
    private List<Long> children;

    // Constructors ////////////////////////////////////////////////////////////

    /**
     * 构造一个全新的内部节点
     */
    InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
              List<Long> children, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
                keys, children, treeContext);
    }

    /**
     * 构造一个持久化到指定page的内部节点
     */
    private InnerNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                      List<DataBox> keys, List<Long> children, LockContext treeContext) {
        try {
            assert (keys.size() <= 2 * metadata.getOrder());
            assert (keys.size() + 1 == children.size());
            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.children = new ArrayList<>(children);
            sync();
        } finally {
            page.unpin();
        }
    }

    // Core API ////////////////////////////////////////////////////////////////

    @Override
    public LeafNode get(DataBox key) {
        BPlusNode child = BPlusNode.fromBytes(metadata, bufferManager, treeContext, children.get(numLessThanEqual(key, keys)));
        return child.get(key);
    }

    @Override
    public LeafNode getLeftmostLeaf() {
        assert (children.size() > 0);
        BPlusNode leftMostLeaf = BPlusNode.fromBytes(metadata, bufferManager, treeContext, children.get(0));
        return leftMostLeaf.getLeftmostLeaf();
    }

    /**
     * put和bulkLoad的辅助方法，用于将键和子节点指针存入到内部节点中
     */
    private Optional<Pair<DataBox, Long>> insert(DataBox key, Long child) {
        // 查看key应该插入的位置
        int index = numLessThanEqual(key, keys);
        // 将key和child插入到对应的位置
        keys.add(index, key);
        children.add(index + 1, child);
        // 查看插入后的键的数量是否超过了2d
        if (keys.size() <= 2 * metadata.getOrder()) {
            // 如果没有溢出，则对page进行更新，并且返回Optional.empty()
            // 对page操作，需要上锁
            sync();
            return Optional.empty();
        } else {
            // 如果溢出，则需要分裂节点，并返回(split_key, right_node_page_num)的Pair
            // 上一级的父节点需要的key
            DataBox split_key = keys.get(metadata.getOrder());
            // 右边子结点需要的key和page number
            List<DataBox> rightKeys = keys.subList(metadata.getOrder() + 1, keys.size());
            List<Long> rightChildren = children.subList(metadata.getOrder() + 1, children.size());
            // 左边原节点需要的key和page number
            keys = keys.subList(0, metadata.getOrder());
            children = children.subList(0, metadata.getOrder() + 1);
            // 更新到page
            sync();
            // 创建新的内部节点
            InnerNode new_rightSibling = new InnerNode(metadata, bufferManager, rightKeys, rightChildren, treeContext);
            return Optional.of(new Pair(split_key, new_rightSibling.getPage().getPageNum()));
        }
    }

    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement
        BPlusNode child = BPlusNode.fromBytes(metadata, bufferManager, treeContext, children.get(numLessThanEqual(key, keys)));
        Optional<Pair<DataBox, Long>> pair = child.put(key, rid);
        if (pair.isPresent()) {
            Pair<DataBox, Long> p = pair.get();
            return insert(p.getFirst(), p.getSecond());
        } else {
            return pair;
        }
    }

    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor) {
        // TODO(proj2): implement
        BPlusNode rightMostChild = BPlusNode.fromBytes(metadata, bufferManager, treeContext, children.get(children.size() - 1));
        // 实际上调用的是LeafNode的bulkLoad
        Optional<Pair<DataBox, Long>> pair = rightMostChild.bulkLoad(data, fillFactor);
        if (pair.isPresent()) {
            // 如果溢出
            DataBox spilt_key = pair.get().getFirst();
            Long child = pair.get().getSecond();
            Optional<Pair<DataBox, Long>> mySplitInfo = insert(spilt_key, child);
            if (mySplitInfo.isPresent()) {
                return mySplitInfo;
            } else {
                // 如果内部节点还是没有分裂，递归调用bulkLoad
                return bulkLoad(data, fillFactor);
            }
        } else {
            // 如果不溢出，不用分裂节点
            return pair;
        }
    }

    @Override
    public void remove(DataBox key) {
        LeafNode leaf = get(key);
        leaf.remove(key);
        return;
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    private BPlusNode getChild(int i) {
        long pageNum = children.get(i);
        return BPlusNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

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
    List<Long> getChildren() {
        return children;
    }

    /**
     * 返回最大数字d，以便具有2d个keys的InnerNode序列化后能适应单个page大小
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // 具有n个keys的内部节点占用的字节数为：1 + 4 + (n * keySize) + ((n + 1) * 8)
        //   - 1字节用于存储isLeaf的
        //   - 4字节用于存储n
        //   - keySize表示用于存储keySchema类型的DataBox的字节数
        //   - 8字节用于存储子节点指针
        //
        //   根据以上条件得到这个不等式：
        //   5 + (n * keySize) + ((n + 1) * 8) <= pageSizeInBytes
        //   求解后得到：
        //   n = (pageSizeInBytes - 13) / (keySize + 8)
        //
        // 其中阶数d = n / 2
        int keySize = keySchema.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + 8);
        return n / 2;
    }

    /**
     * 给定一个按升序排序的列表ys，而numLessThanEqual(x,ys)返回ys中小于或等于x的元素的数量
     * 举个例子：
     *
     * numLessThanEqual(0, Arrays.asList(1, 2, 3, 4, 5)) == 0
     * numLessThanEqual(1, Arrays.asList(1, 2, 3, 4, 5)) == 1
     * numLessThanEqual(2, Arrays.asList(1, 2, 3, 4, 5)) == 2
     * numLessThanEqual(3, Arrays.asList(1, 2, 3, 4, 5)) == 3
     * numLessThanEqual(4, Arrays.asList(1, 2, 3, 4, 5)) == 4
     * numLessThanEqual(5, Arrays.asList(1, 2, 3, 4, 5)) == 5
     * numLessThanEqual(6, Arrays.asList(1, 2, 3, 4, 5)) == 5
     *
     * 当我们沿着B+树向下遍历并需要决定要访问哪个子节点时，此辅助函数很有用
     * 举个例子：假设一个索引节点具有4个键和5个子指针
     *
     * +---+---+---+---+
     * | a | b | c | d |
     * +---+---+---+---+
     * /   |   |   |    \
     * 0   1   2   3     4
     *
     * 如果我们在树中搜索c，那么我们需要访问子节点3。并非巧合的是，还有3个值小于或等于c（即 a、b、c）。
     */
    static <T extends Comparable<T>> int numLessThanEqual(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) <= 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    static <T extends Comparable<T>> int numLessThan(T x, List<T> ys) {
        int n = 0;
        for (T y : ys) {
            if (y.compareTo(x) < 0) {
                ++n;
            } else {
                break;
            }
        }
        return n;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(children.get(i)).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(children.get(children.size() - 1)).append(")");
        return sb.toString();
    }

    @Override
    public String toSexp() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < keys.size(); ++i) {
            sb.append(getChild(i).toSexp()).append(" ").append(keys.get(i)).append(" ");
        }
        sb.append(getChild(children.size() - 1).toSexp()).append(")");
        return sb.toString();
    }

    /**
     * 第0页上具有单个键k和第1、2页上有两个子节点的内部节点将转换为以下 DOT 片段
     *
     * node0[label = "<f0>|k|<f1>"];
     * ... // children
     * "node0":f0 -> "node1";
     * "node0":f1 -> "node2";
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("<f%d>", i));
            ss.add(keys.get(i).toString());
        }
        ss.add(String.format("<f%d>", keys.size()));

        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        String node = String.format("  node%d[label = \"%s\"];", pageNum, s);

        List<String> lines = new ArrayList<>();
        lines.add(node);
        for (int i = 0; i < children.size(); ++i) {
            BPlusNode child = getChild(i);
            long childPageNum = child.getPage().getPageNum();
            lines.add(child.toDot());
            lines.add(String.format("  \"node%d\":f%d -> \"node%d\";", pageNum, i, childPageNum));
        }
        return String.join("\n", lines);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // 当序列化内部节点时，序列化后的内容为：
        //   a.文本值为0(1字节)，表示此节点不是叶节点
        //   b.该内部节点包含的键数为n(4个字节)(比子指针数少 1)
        //   c.n个keys
        //   d.n+1个子节点指针
        //
        // 举个例子：具有一个键（即1）和两个子节点指针（即第3页和第7页）的内部节点表示如下：
        //   +----+-------------+----+-------------------------+-------------------------+
        //   | 00 | 00 00 00 01 | 01 | 00 00 00 00 00 00 00 03 | 00 00 00 00 00 00 00 07 |
        //   +----+-------------+----+-------------------------+-------------------------+
        //    \__/ \___________/ \__/ \_________________________________________________/
        //     a         b        c                           d
        //
        assert (keys.size() <= 2 * metadata.getOrder());
        assert (keys.size() + 1 == children.size());
        int isLeafSize = 1;
        int numKeysSize = Integer.BYTES;
        int keysSize = metadata.getKeySchema().getSizeInBytes() * keys.size();
        int childrenSize = Long.BYTES * children.size();
        int size = isLeafSize + numKeysSize + keysSize + childrenSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 0);
        buf.putInt(keys.size());
        for (DataBox key : keys) {
            buf.put(key.toBytes());
        }
        for (Long child : children) {
            buf.putLong(child);
        }
        return buf.array();
    }

    /**
     * 根据pageNum从指定page中加载内部节点
     */
    public static InnerNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager, LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum);
        Buffer buf = page.getBuffer();

        byte nodeType = buf.get();
        assert (nodeType == (byte) 0);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        int n = buf.getInt();
        for (int i = 0; i < n; ++i) {
            keys.add(DataBox.fromBytes(buf, metadata.getKeySchema()));
        }
        for (int i = 0; i < n + 1; ++i) {
            children.add(buf.getLong());
        }
        return new InnerNode(metadata, bufferManager, page, keys, children, treeContext);
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof InnerNode)) {
            return false;
        }
        InnerNode n = (InnerNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                children.equals(n.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, children);
    }
}
