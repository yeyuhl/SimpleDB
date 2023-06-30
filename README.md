> SimpleDB基于Java 1.8开发，本质上是[CS186 Projects](https://cs186.gitbook.io/project/)的实验

# 创建索引

## DataBox

现代数据库都支持在record（元组）中使用多种数据类型，SimpleDB也不例外。为了一致性和便利性，我们选择在实现语言的默认设置之上构建自己的数据类型内部表示，并用databox表示他们。

一个databox可以容纳以下类型的数据：

- Boolean（1 byte）

- Int（4 bytes）

- Float（4 bytes）

- Long （8 bytes）

- String(N)（N bytes）


## RecordId

一条表中的record，由其page number（页码）和entry number（条目码）唯一标识。因此我们可以用(pageNum, entryNum)组成一个RecordId，在SimpleDB中，我们将在我们的叶节点使用RecordId作为指向数据页中记录的指针。

## index

在SimpleDB中使用B+ Tree作为索引结构，而索引结构的选择有很多，比如Hash表，BST，AVL树，红黑树，B树和B+树等。

- **Hash表**可以通过键(key)即可快速取出对应的值(value)，虽然Hash表查询速度很快，但是不支持顺序和范围查询。假如我们要对表中的数据进行排序或者进行范围查询，那 Hash 索引就不能实现。并且，每次 IO 只能取一个。

- **BST**，即二叉查找树，其特点为：左子树所有节点的值均小于根节点的值；右子树所有节点的值均大于根节点的值；左右子树也分别为二叉查找树。BST在不平衡的时候，会退化成线性链表，查询效率也急剧下降，因此不适合作为索引。

- **AVL树**，针对不平衡的现象，人们发明了AVL树，即自平衡二叉查找树，通过旋转操作来保持平衡。但是AVL树需要频繁的进行旋转操作来保持平衡，因此会有较大的计算开销进而降低了查询性能。并且， 在使用 AVL 树时，每个树节点仅存储一个数据，而每次进行磁盘 IO 时只能读取一个节点的数据，如果需要查询的数据分布在多个节点上，那么就需要进行多次磁盘 IO，由于磁盘IO十分耗时，因此AVL树也不适用于索引。

- **红黑树**，红黑树也是一种自平衡二叉查找树，通过插入和删除节点时进行颜色变换和旋转操作，使得树始终保持平衡状态，其特点为：每个节点非红即黑；根节点总是黑色的；每个叶子节点都是黑色的空节点（NIL 节点）；如果节点是红色的，则它的子节点必须是黑色的（反之不一定）；从根节点到叶节点或空子节点的每条路径，必须包含相同数目的黑色节点（即相同的黑色高度）。但红黑树追求的是大致的平衡，树的高度较高，这可能会导致一些数据需要进行多次磁盘 IO 操作才能查询到，这也是 MySQL 没有选择红黑树作为的主要原因。不过红黑树的插入和删除操作效率大大提高了，因为红黑树在插入和删除节点时只需进行 O(1) 次数的旋转和变色操作，即可保持基本平衡状态，它在内存方面应用较多。

- **B 树 & B+树**，B 树也称 B-树，全称为 **多路平衡查找树** ，B+ 树是 B 树的一种变体。B 树和 B+树中的 B 是 `Balanced` （平衡）的意思。目前大部分数据库系统及文件系统都采用 B-Tree 或其变种 B+Tree 作为索引结构。

  ![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGov2-7a83d0068331c5fe82ae2557b97e52d8_1440w.webp)

  **B 树& B+树两者有何异同呢？**

  - B 树的所有节点既存放键(key) 也存放数据(data)，而 B+树只有叶子节点存放 key 和 data，其他内节点只存放 key。
  - B 树的叶子节点都是独立的；B+树的叶子节点有一条引用链指向与它相邻的叶子节点。
  - B 树的检索的过程相当于对范围内的每个节点的关键字做二分查找，可能还没有到达叶子节点，检索就结束了。而B+树的检索效率就很稳定了，任何查找都是从根节点到叶子节点的过程，叶子节点的顺序检索很明显。
  - 在 B 树中进行范围查询时，首先找到要查找的下限，然后对 B 树进行中序遍历，直到找到查找的上限；而B+树的范围查询，只需要对链表进行遍历即可。

  综上，B+树与 B 树相比，具备更少的 IO 次数、更稳定的查询效率和更适于范围查询这些优势。


在 MySQL 中，MyISAM 引擎和 InnoDB 引擎也都是使用 B+ Tree 作为索引结构，但是，两者的实现方式不太一样。

> MyISAM 引擎中，B+Tree 叶节点的 data 域存放的是数据记录的地址。在索引检索的时候，首先按照 B+Tree 搜索算法搜索索引，如果指定的 Key 存在，则取出其 data 域的值，然后以 data 域的值为地址读取相应的数据记录。这被称为“**非聚簇索引（非聚集索引）**”。
>
> InnoDB 引擎中，其数据文件本身就是索引文件。相比 MyISAM，索引文件和数据文件是分离的，其表数据文件本身就是按 B+Tree 组织的一个索引结构，树的叶节点 data 域保存了完整的数据记录。这个索引的 key 是数据表的主键，因此 InnoDB 表数据文件本身就是主索引。这被称为“**聚簇索引（聚集索引）**”，而其余的索引都作为 **辅助索引** ，辅助索引的 data 域存储相应记录主键的值而不是地址，这也是和 MyISAM 不同的地方。在根据主索引搜索时，直接找到 key 所在的节点即可取出数据；在根据辅助索引查找时，则需要先取出主键的值，再走一遍主索引。 因此，在设计表的时候，不建议使用过长的字段作为主键，也不建议使用非单调的字段作为主键，这样会造成主索引频繁分裂。

回到SimpleDB中，我们需要实现以下重要类来构建index：

- **BPlusTree**：该文件包含管理 B+ 树结构的类。每个 B+ 树都将 DataBox 类型的键（表中的单个值或“单元格”）映射到 RecordId 类型的值（数据页上记录的标识符）。

- **BPlusNode**：一个B+节点表示B+树中的一个节点，包含与BPlusTree类似的get、put、delete等方法。 BPlusNode 是一个抽象类，实现为 LeafNode 或 InnerNode。

  - **LeafNode**：叶节点是没有后代的节点，它包含指向表中相关record的key和record ID对，以及指向其右兄弟节点的指针。

  - **InnerNode**：内部节点是存储指向子节点（它们本身可以是内部节点或叶节点）的键和指针（页码）的节点。

- **BPlusTreeMetadata**：index文件夹下包含的一个类，用于存储树的顺序和高度等有用信息。可以使用上面列出的所有类中可用的 this.metadata 实例变量来访问此类的实例。


此外有一些实现的注意事项：

- 一般来说，B+树是支持重复键的。但是，我们实现的 B+ 树要做到不支持重复键。每当插入重复键时，都需要抛出异常。

- 我们实现的B+树，仅假设内部节点和叶节点可以在单个page（数据页）上序列化，不用考虑多个pages的情况。

- 在SimpleDB中，delete不会重新平衡树。因此，对于d阶B+树中的所有非根叶节点，d和2d的条目之间的不变量（invariant）被打破。实际的B+树在删除后会重新平衡，但为了简单起见，我们不会在该数据库中对树重新平衡。


由于BPlusNdoe是LeafNode和InnerNode的父类，因此关键在于在子类中实现BPlusNode的如下方法：

```java
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
```

**public abstract LeafNode get(DataBox key);**

理解get的核心就是通过key获取LeafNode后，就能很简单的实现其功能。如果是InnerNode的话，那么根据key获得pagenum，创建新的临时节点，调用其get方法，这样层层递进，最后肯定是叶节点调用自身的get，而叶节点就是要返回的对象，直接返回this即可。

```java
    /**
     * node.getLeftmostLeaf()返回以node为根的子树中最左边的叶子节点
     * 在上面的例子中，inner.getLeftmostLeaf()将返回leaf0 ，而leaf1.getLeftmostLeaf()将返回leaf1
     */
```

**public abstract LeafNode getLeftmostLeaf();**

对于InnerNode，就是获取子结点中pagenum最小的节点，同样的创建临时节点调用getLeftmostLeaf方法，最后叶节点调用自身getLeftmostLeaf，返回this。

```java
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
```

**public abstract Optional<Pair< DataBox, Long >> put(DataBox key, RecordId rid);**

需要考虑溢出问题。对于InnerNode，首先通过key判断要插入的entry要插到哪里；把entry插入后，判断有无溢出(keys.size()是否超过了2d)；如果溢出，则分裂，左节点为原节点，保存前d条entries，右节点为新节点存储d+2及之后的entries，在keys中索引为d的split_key将存储到更上一层的节点，最后更新page；如果没有溢出，则更新page。而LeafNode总体流程基本一致，就是分裂阶段存在不同，分裂成两个叶节点，左节点保存前d条entries，而右节点保存d+1及之后的entries。

```java
    /**
     * node.bulkLoad(data, fillFactor)是按照填充因子fillFactor将data中的record批量加载到以node为根的子树中的方法
     * 这个方法十分类似于node.put，但有些地方不同：
     * 1.叶子节点不是填充到2*d+1然后分裂，而是填充到比fillFactor多1条记录，然后通过创建一个只包含一条记录的右边兄弟节点来 "分裂"（留下具有所需填充系数的原始节点）
     * 2.内部节点应该反复尝试批量加载到最右边的子节点，直到内部节点已经满了（在这种情况下，它应该分裂）或者没有更多的数据了
     * fillFactor用来表示叶节点有多满，假如fillFactor为1，意味着叶节点应该完全填满，如果fillFactor为0.5，意味着叶节点应该填满一半
     * fillFactor高则范围查询性能更好，但会增加访问特定record的I/O成本，计算叶节点多满应该向上取整，d=5，fillFactor=0.75，叶节点应该4/5满
     */
```

**public abstract Optional<Pair< DataBox, Long >> bulkLoad(Iterator<Pair< DataBox, RecordId >> data, float fillFactor);**

跟get方法十分类似，但是涉及到fillFactor会有所不同。对于InnerNode，实际上调用的是叶节点的bulkLoad，把entries填充到最右边的叶节点。InnerNode的bulkLoad更多是用来检测InnerNode在这个过程是否溢出，如果溢出就要分裂。对于LeafNode，关键是用fillFactor控制每个LeafNode中entries的占比，比如fillFactor为0.5，说明叶节点只有一半的空间能装载entries。一边加载数据一边检测是否到达这个界限，如果触及就分裂节点。

```java
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
```

**public abstract void remove(DataBox key);**

由于不考虑重新平衡B+树，因此remove实现起来十分简单，查询哪个叶节点有对应key，然后从删除叶节点中的的key和对应的record id。

这一部分的框架图如下所示：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230619164330.png)

# Joins和查询优化

## common/iterator

common/iterator目录中包含一个称为BacktrackingIterator（回溯迭代器）的接口。实现这个借口的迭代器，可以在迭代期间标记一个点，并且通过重置到这个标记点。

举个例子：迭代[1,2,3]

```java
BackTrackingIterator<Integer> iter = new BackTrackingIteratorImplementation();
iter.next();     // returns 1
iter.next();     // returns 2
iter.markPrev(); // marks the previously returned value, 2
iter.next();     // returns 3
iter.hasNext();  // returns false
iter.reset();    // reset to the marked value (line 5)
iter.hasNext();  // returns true
iter.next();     // returns 2
iter.markNext(); // mark the value to be returned next, 3
iter.next();     // returns 3
iter.hasNext();  // returns false
iter.reset();    // reset to the marked value (line 11)
iter.hasNext();  // returns true
iter.next();     // returns 3
```

## query/QueryOperator.java

> 参考来源[Join processing in database systems with large main memories](https://dl.acm.org/doi/10.1145/6314.6315)

query目录包含所谓的**QueryOperator（查询运算符）**。对数据库的单一查询可以表示为这些运算符的组合。所有操作符都扩展了QueryOperator类，并实现了Iterable< Record >接口。Scan Operators（扫描运算符）从一个表中获取数据。其余的运算符接受一个或多个输入运算符，对输入进行转换或组合（例如，投射列、排序、连接），并返回一个records的集合。

### Join Operators

JoinOperator.java是所有连接运算符的基类。实现连接算法时，不应该直接处理Table对象或TransactionContext对象（除了将它们传递到需要它们的方法中）。

#### 1.Nested Loop Joins（嵌套循环连接）

嵌套循环连接（Nested Loop Join）是一种最基本的连接实现算法。它先从**外部表**（驱动表）中获取满足条件的数据，然后为每一行数据遍历一次**内部表**（被驱动表），获取所有匹配的数据。值得注意的是，DBMS总是希望外部表是较小的表，这样在内存中缓存尽量多的外部表，在遍历内部表时也可以使用索引加速。

**1）Simple Nested Loop Join (SNLJ)**

SNLJ的简单之处在于实现起来简单，假设我们有两个表R(外)和S(内)，从R中取出record ri，然后从S中遍历，找出与之匹配的Sj，找到后返回连接后的record。伪代码如下：

```
for each record ri in R:
    for each record sj in S:
        if θ(ri,sj):
            yield<ri,sj>
```

**2）Page Nested Loop Join (PNLJ)**

SNLJ的弊端很明显，我们并不希望对S所有pages进行扫描，那么我们可以按page为大小来进行扫描。先从R中读取一个page，再从S中读取一个page，然后两个page之间匹配，如果S没有匹配完R的page，那么从S中获取下一个page，如此重复，即为**页面嵌套循环连接**(Page Nested Loop Join)。伪代码如下：

```
for each page pr in R:
    for each page ps in S:
        for each record ri in pr:
            for each record sj in ps:
                if θ(ri,sj):
                    yield<ri,sj>
```

**3）Block Nested Loop Join (BNLJ)**

PNLJ虽然对SNLJ进行了优化，但是没有充分利用buffer。假如我们有B个buffer pages，但PNLJ实际上只用了三个，一个给R，一个给S，一个是output buffer。由于我们希望尽可能减少S的读取次数，因此我们保留两个buffer pages给S和output buffer，其余B-2个pages全部分配给R。这就是**块嵌套循环连接**（Block Nested Loop Join），其核心思想在于，希望利用缓冲区来降低IO成本，因此我们为一个R中的块保留尽可能多的pages，每个块只在S的每个page读取一次，较大的块带来更低的IO。而对于R的每个块，将S中的所有records与块中所有records进行匹配。伪代码如下：

```
for each block of B-2 pages Br in R:
    for each page ps in S:
        for each record ri in Br:
            for each record sj in ps:
                if θ(ri,sj):
                    yield<ri,sj>
```

在实现中，需要关注以下三个方法：

- **fetchNextLeftBlock**：从 leftSourceIterator 获取左表页面的下一个非空块。

- **fetchNextRightPage**：从 rightSourceIterator 获取右表的下一个非空页面。

- **fetchNextRecord**：获取连接输出的下一条record，并且要考虑以下四种情况：

  - 遍历完右page中的records，都没匹配上左block当前的record，获取左Block下一条record

  - 遍历完右page中的records，左block中所有records没有与之匹配的，获取右表中的下一个page

  - 遍历完右表中的records，左block中所有records没有与之匹配的，获取左表中的下一个block

  - 两表剩下的records都匹配不上


**4）Index Nested Loop Join(INLJ)**

虽然该数据库并不实现INLJ，但是可以介绍一下。有时候BNLJ并不是最好的方法，比如说S上有一个索引，而这个索引刚好在我们要连接的字段上，此时在S中查找匹配的字段就会非常快，这被称为**索引嵌套循环连接**(Index Nested Loop Join)，其伪代码如下：

```
for each record ri in R:
    for each record sj in S where θ(ri,sj)==true:
        yield<ri,sj>
```

#### 2.Hash Joins（哈希连接）

由于hash函数可以只通过一次运算就将任意键值映射到固定大小、固定值域的hash值，因此也是一种实现连接的方法。在实践中，针对等值 join 所需的等值比较，一般数据库系统会仔细选择和优化 hash 函数或函数簇，使其能够快速缩小需要和一个键值进行等值比较的其它键值的数量或范围，从而实现了通过减少计算量、内外存访问量等手段来降低 join 算法的执行开销。

**1）Simple Hash Join (SHJ)**

一般分为两步，建立阶段（build phase)和探测阶段（probe phase)：

- **Build**：选择两个表中较小的一个，即外部表R（一般称其为build relation），使用一个或一簇 hash 函数将其中的每一条record中连接两张表的那一列的值计算为一个 hash 值，然后根据 hash 值将该record插入到一张表中，这张表就叫做 hash 表。
- **Probe**：选择两个表中较大的一个，即内部表S（一般称为probe relation），针对其中的每一条record，使用和 build 中相同的 hash 函数，计算出相应的 hash 值，然后根据 hash 值在 hash 表中寻找到需要比较的record，逐一比较，如果找到匹配项则输出。

**2）Grace Hash Join (GHJ)**

假如R的哈希表太大，内存放不下，DBMS可能会随机将哈希表页换出，性能将大打折扣。所以我们要考虑对其进行分区。

首先对两个表进行分区，然后对连接两张表的那一列的值进行哈希处理，并将其添加到正确的分区中。最后对指定的分区执行**Build**和**Probe**，如果该分区不能执行（getNumPages()大于B - 2，即超出约束的内存），则需要递归地应用GHJO来进一步来划分分区直至能执行**Build**和**Probe**为止。

#### 3.External Sort

**Sort Merge Join**的第一步是对两个输入关系进行排序。因此，在实现Sort Merge Join 之前，必须首先实现外部排序算法。

下面和代码中提及的“run”，仅仅是外部合并排序中已排序的records的序列。这在 SortOperator 中由 Run 类（位于 query/disk/Run.java 中）表示。由于外部合并排序中的runs可以跨越许多页（并最终跨越整个表），因此 Run 类不会将其所有数据保留在内存中。相反，它创建一个临时表并将其所有数据写入临时表（由缓冲区管理器自行决定将其具体化到磁盘）。

需要实现的核心方法如下：

- **sortRun(run)**：在内存中对传入的数据进行排序。

- **mergeSortedRuns(runs)**：在给定排序后的runs列表的情况下返回新run。

- **mergePass(runs)**：执行外部合并排序的单个合并过程，给出前一个过程中所有已排序runs的列表。

- **sort()**：从头到尾运行外部合并排序，并返回最终运行的排序数据。


#### 4. Sort Merge Join

有时我们希望连接表在指定列上进行排序，此时**排序合并连接** (Sort Merge Join)就十分有用。首先对R和S进行排序，排序完后：

- 对于R和S中的records，如果ri < sj，那么优先遍历R；如果ri > sj，那么优先遍历S。即两者小的先遍历，直到匹配成功。

- 假设上面匹配成功，那么我们得到< ri,sj >，我们对sj这个点进行标记**marked(S)**。然后检查紧随其后的records(sj,sj+1,sj+2,etc)，直到我们找到与ri不匹配的record（即读取S中所有和ri匹配的records）。

- 现在我们可以读取R的下一条record，并且S回溯到marked(S)，重新进行步骤1。由于R和S都是有序的，因此R中未来任何records如果成功匹配，就不可能是与 marked(S) 之前的records匹配成功。


其伪代码如下：

```
do {
    if (!mark) {
        while (r < s) { advance r }
        while (r > s) { advance s }
        // mark start of “block” of S
        mark = s
     }
    if (r == s) {
        result = <r, s>
        advance s
        yield result
    }
    else {
        reset s to mark
        advance r
        mark = NULL
    }
}
```

为简单起见，实现SMJ不应该对其进行优化（排序的最终合并过程与连接同时发生）。因此，在SMJ的排序阶段应该使用SortOperator进行排序。需要实现 **SortMergeOperator** 以及其内部类 **SortMergeIterator** 。此外提一嘴，其合并体现在有序records的合并。

### Scan Operators

扫描运算符用于直接从表中获取数据。

**SequentialScanOperator.java**：用来获取表名并提供该表包含所有records的迭代器。**IndexScanOperator.java**：用来获取表名、列名、PredicateOperator (>、<、<=、>=、=) 和一个值。指定的列必须有索引才能使该运算符起作用。如果有索引，索引扫描将利用索引有效地生成该列中满足给定谓词和值（例如 salaries.yearid >= 2000）的records。

### Special Operators

其余的运算符不属于特定类别，而是执行某些特定目的。

**​SelectOperator.java**：相当于关系代数的 σ 运算符。该运算符获取列名、PredicateOperator（>、<、<=、>=、=、!=）和一个值。它只会从满足谓词的源运算符生成records，例如 (yearid >= 2000)。

**ProjectOperator.java**：相当于关系代数的π运算符。该运算符获取列名列表并过滤掉未列出的任何列。

**​SortOperator.java**：按排序顺序从源运算符中生成records。

### Other Operators

**​MaterializeOperator.java**：将源运算符物化（存储）到临时表中，然后对临时表进行顺序扫描。主要用于测试控制IO何时发生。

**GroupByOperator.java**：该运算符接收列名并生成源运算符的records，但records按其值分组，并且每个record由标记record分隔。例如，如果源运算符具有单例record[0,1,2,1,2,0,1]，则 group by 运算符可能会生成 [0,0,M,1,1,1,M,2,2]其中 M 是标记record。

## query/QueryPlan

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230628101842.png)

这是**火山模型**(volcano model)，其中运算符彼此分层，每个运算符在需要生成下一个输出元组时向输入运算符请求元组。请注意，每个运算符仅根据需要从其输入运算符中获取元组，而不是一次全部获取！

**QueryPlan**是查询运算符的组合，它描述了查询的执行方式。回想一下，SQL 是一种声明性语言，即用户不指定查询如何运行，而只指定查询应返回什么。因此，对于给定的查询通常有许多可能的查询计划。

QueryPlan类代表一个查询。数据库的用户使用公共方法（例如 join()、select() 等）创建查询，然后调用执行来生成查询的查询计划，并返回结果数据集上的迭代器（这不是完全具体化：迭代器根据请求生成每个元组）。

### SelectPredicate

SelectPredicate 是 QueryPlan.java 中的一个辅助类，它存储有关用户已应用的选择谓词的信息，例如someTable.col1 < 186。选择谓词有四个可以访问的值：

- tableName和columnName指定谓词适用于哪个表的哪一列。

- 运算符表示正在使用的运算符的类型（例如 <、<=、> 等...）。

- value 是一个 DataBox，其中包含一个常量值，应根据该常量值对列进行评估（在上面的示例中，该值是 186）。


查询涉及到的所有选择谓词都存储在 selectPredicates 实例变量中。

### JoinPredicate

JoinPredicate 是 QueryPlan.java 中的一个辅助类，它存储有关表连接在一起的条件的信息，例如：leftTable.leftColumn = rightTable.rightColumn。该数据库中的所有联接都是等值联接。 JoinPredicates 有五个值：

- joinTable：要加入的表之一的名称，仅用于 toString()。

- leftTable：等式左边的表名。

- leftColumn：等式左边的列名。

- rightTable：等式右边的表名。

- rightColumn：等式右边的列名。


查询涉及到的所有连接谓词都存储在 joinPredicates 实例变量中。

## Query Optimization

首先看如下流程图：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230630101454.png)

前面说过，一个QueryPlan只是一个操作序列，比如上面，它将两个表连接在一起，然后过滤掉行，最后仅投影它想要的列。这种方式能保证获取正确的查询结果，但事实上，数据库可能并不按照这样的顺序工作，它会改变操作的执行顺序，已获得最佳性能。在这里，我们用IO次数来衡量性能，查询优化就是找到一个IO次数最小的QueryPlan。

### Common Heuristics(启发式)

对于复杂的查询而言，查询计划太多无法全部分析。因此我们需要某种方法来减少我们实际考虑的计划数量，所以我们需要使用一些启发式规则：

- 对于projects(π)和selects(σ)尽可能往下推（即投影运算和选择运算尽可能先做）

- 仅考虑left deep plans

- 不考虑cross joins，除非这是唯一选择


对于第一条启发式规则，之所以要先做投影运算和选择运算，这是为了减少其他运算符要处理的pages数目。project消除了列，select减少了要查询的行数，这样一来行变小了，在一个page上能装下更多行，要处理的pages也减少了。

对于第二条启发式规则，仅考虑left deep plans中的left deep plans是指一个连接中所有的右表都是基表的计划（即右边永远不是连接本身的结果，只能是原始表之一），如下图给出了什么是left deep plans，什么不是left deep plans：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230630154731.png)

仅考虑left deep plans的好处在于，一是可以大大减少计划空间。计划空间是关系数的阶乘，仅考虑left deep plans比起考虑每个计划时计划空间要小得多。二是这些计划可以被完全流水线化，这意味着我们可以将页面一个一个地传递给下一个连接操作者——我们实际上不需要将连接的结果写入磁盘。

对于第三条启发式规则，不使用cross joins是因为cross joins会产生大量的pages，这使得cross joins上面的运算符要执行许多IO，这无疑是高成本的。

### Single Table Access Selection（Pass 1）

想要实现优化，先考虑单表的情况。在SimpleDB中，只考虑两种类型的表扫描：全表顺序扫描（SequentialScanOperator）和索引扫描（IndexScanOperator），后者需要一个索引和对列的过滤谓词。

首先要计算顺序扫描的预估I/O成本，因为我们默认首选顺序扫描。然后，如果表的任一索引在选择谓词所应用的列上，计算对该列进行索引扫描的预估I/O成本。如果其中有任何一个比顺序扫描更有效，就选择最好的一个。

根据I/O成本，选择如下：

1. Full Scan players (100 I/Os)

2. Index Scan players.age (90 I/Os)

3. Index Scan players.teamid (120 I/Os)

4. Full Scan teams (300 I/Os)

5. Index Scan teams.record (400 I/Os)


当然还有基于启发式规则的优化，即

- 对于小关系，使用全表顺序扫描，即使选择列上有索引。

- 对于大关系，如果选择条件是“主码=值”的查询，查询结果最多是一个元组，可以选择主码索引。

- 对于大关系，如果选择条件是“非主属性=值”的查询，并且选择列上有索引，则要估算查询结果的元组数目，如果比例较小(<10%)则使用索引扫描方法，否则就使用全表顺序扫描。

- 对于大关系，如果选择条件是属性上的非等值查询或者范围查询，并且选择列上有索引，同样要估算查询结果的元组数目，如果选择率<10%可以使用索引扫描方法，否则使用全表顺序扫描。

- 对于大关系，如果用AND连接的合取选择条件，有设计这些属性的组合索引，则优先采用组合索引扫描方法；如果某些属性上有一般索引，则可以用索引扫描方法，否则使用全表顺序扫描。

- 对于大关系，如果用OR连接的析取选择条件，一般使用全表顺序扫描。


**QueryOperator**中有一个stimateIOCost方法，用来估计执行此查询运算符的 IO 成本，在子类中会实现，在实现**QueryPlan**的**minCostSingleAccess**方法时会用到。

### Join Selection (Pass i > 1)

对于每一个pass i，我们尝试将i个表连接在一起，使用pass i-1和pass 1的结果。例如，在pass 2时，我们将尝试将两个表连接在一起，每个表都来自pass 2。在pass 5时，我们将尝试将总共5个表连接在一起。我们将从pass 4中得到其中的4张表（它知道如何将4张表连接在一起），我们将从pass 1中得到剩余的表。请注意，这执行了我们的left deep plans启发式。我们总是用一个基础表来连接一组连接的表。

Pass i将为所有长度为i的表集产生至少一个查询计划，这些表集可以在没有交叉连接的情况下被连接（假设至少有一个这样的表集）。就像在pass 1中一样，它将推进每个集合的最优计划，以及每个集合的每个interesting顺序的最优计划（如果存在）。当试图用pass 1的一张表连接一组表时，我们考虑数据库已经实现每个连接。

这些连接中只有一个会产生排好序的输出——SMJ，所以要想有一个interesting的顺序，唯一的办法就是对这组中的最后一个连接使用SMJ。SMJ的输出将根据连接条件中的列进行排序。

然而，SNLJ和INLJ都可以保留左边关系的排序顺序。因为这两种方法在转移到左边关系的下一个元组之前对左边关系的单个元组执行了所有的连接，所以左边关系的排序被保留了。

GHJ、PNLJ和BNLJ从未产生interesting的顺序。GHJ的输出顺序是基于探测关系中元组的顺序，因为我们遍历了探测关系中的每个元组以便在另一个关系的内存哈希表中找到匹配。然而，由于探测关系本身已经通过哈希进行了分区，所以不能保证探测关系在各分区中是否有序，从而不能保证输出的整体顺序。PNLJ和BNLJ类似于SNLJ，但是它们在转到左边关系中的下一个元组之前不会生成左边关系中单个元组的所有连接输出。相反，它们从左侧关系中读取元组的范围，并在这些范围上与右侧关系中的元组块进行匹配，所以它们不保留顺序。

举个例子：

SELECT *
from a inner join b
ON A.aid = B.bid
INNER JOIN C
ON b.did = c.cid
ORDER BY c.cid；

在pass 2中，我们将返回哪些表集的查询计划？在这一pass中，我们将考虑的唯一表集是{A，B}和{B，C}。因为没有连接条件，所以我们不考虑{A, C}，并且我们的启发式规则也规定不要使用交叉连接。为了简化问题，假设数据库中只实现了SMJ和BNLJ，并且pass 1只为每个表返回一个全表扫描。将考虑的以下连接（成本是为问题而制定的。在实践中，将使用选择性估计和连接成本公式）：

1. A BNLJ B (估计成本：1000)

2. B BNLJ A (估计成本：1500)

3. A SMJ B (估计成本：2000)

4. B BNLJ C (估计成本：800)

5. C BNLJ B (估计成本：600)

6. C SMJ B (估计费用：1000)


连接1、5和6将被推进。1是集合{A，B}的最佳连接。5对集合{B，C}来说是最优的。6是一个interesting的顺序，因为有一个ORDER BY c.cid。之所以不推进3，是因为A.aid和B.bid在这个连接之后没有被使用，所以这个顺序并不interesting。

现在让我们进入pass 3。我们将考虑以下的连接（同样是连接成本的构成）：

join1 {A，B} BNLJ C (估计成本：10,000)

join1 {A，B} SMJ C (估计成本：12,000)

join5 {B，C} BNLJ A (估计成本：8,000)

join5 {B，C} SMJ A (估计成本：20,000)

join6 {B，C} BNLJ A (估计成本：22,000)

join6 {B，C} SMJ A (估计成本：18,000)

注意，现在我们不能改变连接顺序，因为我们只考虑left deep plan，所以基础表必须在右边。

现在唯一能推进的计划是2和3。对于所有3个表的集合来说，3是最佳的整体。2对于所有3个表的集合来说是最优的，在C.cid上有一个interesting的顺序（这仍然是interesting的，因为我们还没有评估ORDER BY子句）。2产生在C.cid上排序的输出的原因是连接条件是B.did = C.cid，所以输出将在B.did和C.cid上排序（因为它们是一样的）。4和6不会产生在C.cid上排序的输出，因为它们将把A加入到连接表的集合中，所以条件将是A.aid = B.bid。A.aid和B.bid都没有在查询的其他地方使用，所以它们的排序对我们来说没有意义。

对于i>1，动态编程算法的第i次传递吸收了所有可能的i-1个连接表的最优计划（除了那些涉及笛卡尔积的），并返回所有可能的i个连接表的最优计划（同样不包括那些笛卡尔积的）。

我们将两次传递之间的状态表示为从字符串集（表名）到相应的最优查询操作器的映射。需要实现**QueryPlan**的**minCostJoins**方法，以实现上述搜索算法的第i遍（i>1）的逻辑。

该方法给定一个从i-1个表的集合到连接这些i-1个表的最佳计划的映射，返回一个从i个表的集合到连接所有i个表的集合的最佳左深计划的映射（除了那些有笛卡尔积）。当用户调用QueryPlan的join方法时，应添加的显式连接条件列表来识别潜在的连接。

### Optimal Plan Selection

最后的最后，要实现optimizer的最外层驱动方法——QueryPlan中的execute方法。该方法会利用到前面实现的两个方法，并且需要添加剩余的group by和project运算符，这些运算符是查询的一部分，但是还没添加到查询计划中。值得注意的是，QueryPlan中的表被保存在变量tableNames中。