package io.github.yeyuhl.database.index;

import io.github.yeyuhl.database.common.Buffer;
import io.github.yeyuhl.database.common.Pair;
import io.github.yeyuhl.database.concurrency.LockContext;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.memory.BufferManager;
import io.github.yeyuhl.database.memory.Page;
import io.github.yeyuhl.database.table.RecordId;

import java.util.Iterator;
import java.util.Optional;

/**
 * B+树节点的抽象类，可以是内部节点，也可以是叶子节点
 *
 * @author yeyuhl
 * @since 2023/6/20
 */
abstract class BPlusNode {
    /**
     * node.get(k)返回对node进行查询时，k可能所在的叶节点
     * 例如，考虑以下B+树（为简洁起见，只显示键；省略了记录id）。
     * <p>
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     * <p>
     * inner.get(x)应该返回：
     * leaf0 when x < 10
     * leaf1 when 10 <= x < 20
     * leaf2 when x >= 20
     */
    public abstract LeafNode get(DataBox key);

    /**
     * node.getLeftmostLeaf()返回以node为根的子树中最左边的叶子节点
     * 在上面的例子中，inner.getLeftmostLeaf()将返回leaf0 ，而leaf1.getLeftmostLeaf()将返回leaf1
     */
    public abstract LeafNode getLeftmostLeaf();

    /**
     * node.put(k, rid)将键值对（k，rid）插入到以node为根的子树中，并且需要考虑以下两种情况：
     * 1.如果插入后不会导致node溢出，则返回Optional.empty()
     * 2.如果插入后会导致node溢出，那么node就会被分割为一个左节点和右节点并返回一对(split_key, right_node_page_num)，
     * 其中right_node_page_num是新创建的右节点的页码，而split_key取决于node是一个内部节点还是一个叶子节点
     * <p>
     * 举个例子：
     * 1.往前面的B+树中插入key为4的entry，满足情况1，插入后的B+树如下：
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  2 |  3 |  4 |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     * <p>
     * 2.在上面的基础上，在插入key为5的entry，满足情况2，插入后的B+树如下：
     *                          inner
     *                          +--+--+--+--+
     *                          | 3|10|20|  |
     *                          +--+--+--+--+
     *                         /   |  |   \
     *                 _______/    |  |    \_________
     *                /            |   \             \
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   | 1| 2|  |  |->| 3| 4| 5|  |->|11|12|13|  |->|21|22|23|  |
     *   +--+--+--+--+  +--+--+--+--+  +--+--+--+--+  +--+--+--+--+
     *   leaf0          leaf3          leaf1          leaf2
     * 可以看到leaf0由于溢出，创建了一个新的兄弟节点leaf3，d个entries在leaf0(d是5/2=2)，d+1及之后的entries在leaf3
     * <p>
     * 当一个内部节点分裂时，前d个entries在原来节点，最后d个entries在新创建的节点，而中间的entries则向上移动（不是复制）
     * 举个例子：
     * +---+---+---+---+
     * | 1 | 2 | 3 | 4 | 5
     * +---+---+---+---+
     * 分裂后：
     * +---+---+---+---+  +---+---+---+---+
     * | 1 | 2 |   |   |  | 4 | 5 |   |   |
     * +---+---+---+---+  +---+---+---+---+
     * 此时split_key为3
     * <p>
     * 除了以上提及的情况，不要以任何其他方式重新分配entries，例如不要在节点之间移动entries以避免拆分
     * 此外，SimpleDB不支持具有相同key的两条重复entries，如果有重复的key插入到一个叶子节点中，B+树并不会发生改变并且会抛出BPlusTreeException
     */
    public abstract Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid);

    /**
     * node.bulkLoad(data, fillFactor)是按照填充因子fillFactor将data中的record批量加载到以node为根的子树中的方法
     * 这个方法十分类似于node.put，但有些地方不同：
     * 1.叶子节点不是填充到2*d+1然后分裂，而是填充到比fillFactor多1条记录，然后通过创建一个只包含一条记录的右边兄弟节点来 "分裂"（留下具有所需填充系数的原始节点）
     * 2.内部节点应该反复尝试批量加载到最右边的子节点，直到内部节点已经满了（在这种情况下，它应该分裂）或者没有更多的数据了
     * fillFactor用来表示叶节点有多满，假如fillFactor为1，意味着叶节点应该完全填满，如果fillFactor为0.5，意味着叶节点应该填满一半
     * fillFactor高则范围查询性能更好，但会增加访问特定record的I/O成本，计算叶节点多满应该向上取整，d=5，fillFactor=0.75，叶节点应该4/5满
     */
    public abstract Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data, float fillFactor);

    /**
     * node.remove(key)从以node为根的子树中删除键值对（key，rid）
     * 如果key不在子树中，那么不做任何操作，并且remove后不用重新平衡树，只需删除（key，rid）即可
     * 举个例子：
     * 对一开始出现的子树调用inner.remove(2)后，子树变为：
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |  1 |  3 |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     * 再接连调用inner.remove(1)和inner.remove(3)后，子树变为：
     *                               inner
     *                               +----+----+----+----+
     *                               | 10 | 20 |    |    |
     *                               +----+----+----+----+
     *                              /     |     \
     *                         ____/      |      \____
     *                        /           |           \
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   |    |    |    |    |->| 11 | 12 | 13 |    |->| 21 | 22 | 23 |    |
     *   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
     *   leaf0                  leaf1                  leaf2
     */
    public abstract void remove(DataBox key);

    /**
     * 获取该节点所持久化的page
     */
    abstract Page getPage();

    /**
     * S-exps（或者叫sexps）是一种编码嵌套树状结构的紧凑方式（有点像JSON是一种编码嵌套字典和列表的方式）
     * node.toSexp()返回以node为根的子树的sexp编码
     * 举个例子：
     *                      +---+
     *                      | 3 |
     *                      +---+
     *                     /     \
     *   +---------+---------+  +---------+---------+
     *   | 1:(1 1) | 2:(2 2) |  | 3:(3 3) | 4:(4 4) |
     *   +---------+---------+  +---------+---------+
     * 用sexps表示该树：
     * (((1 (1 1)) (2 (2 2))) 3 ((3 (3 3)) (4 (4 4))))
     */
    public abstract String toSexp();

    /**
     * node.toDot()返回DOT文件的片段，该片段绘制以node为根的子树
     */
    public abstract String toDot();

    /**
     * 序列化
     */
    public abstract byte[] toBytes();

    /**
     * 从页号为pageNum的页中加载一个BPlusNode
     */
    public static BPlusNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                      LockContext treeContext, long pageNum) {
        Page p = bufferManager.fetchPage(treeContext, pageNum);
        try {
            Buffer buf = p.getBuffer();
            byte b = buf.get();
            // 根据isLeaf判断是叶节点还是内部节点
            if (b == 1) {
                return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else if (b == 0) {
                return InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            } else {
                String msg = String.format("Unexpected byte %b.", b);
                throw new IllegalArgumentException(msg);
            }
        } finally {
            p.unpin();
        }
    }
}
