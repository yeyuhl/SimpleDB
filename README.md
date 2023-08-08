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

# 并发

## Transactions

考虑多人同时访问数据库（并发），对某一数据进行读写的情况，会产生以下问题：

- **读取不一致（读写冲突）**：另一个用户要对数据进行更新，还没更新完，另一用户就对该数据进行读取。

- **丢失更新（写入冲突）**：两个用户同时对某一数据进行更新。

- **脏读（读写冲突）**：某一用户读取了未commit（被回滚）的数据。比如说某一用户更新了数据，但被中止操作，要求回滚。在回滚前有一用户读取了这部分数据。

- **不可重复读（读写冲突）**：某一用户读取同一数据的两个不同值，因为另一个用户在两次读取之间更新了该数据。


![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230709200144.png)

但并发并不是一无是处，并发可以**增加数据库吞吐量**（每秒事务数）和**减少延迟**（每个事务响应时间）。

为了解决以上问题，我们提出了**事务**这一概念，即用户定义的一个数据库操作序列，要么做要么不做，不存在你中间状态，是一个不可分割的工作单位。定义事务的语句一般有三条：**BEGIN TRANSACTION、COMMIT、ROLLBACK**，事务通常是以BEGIN TRANSACTION开始，以COMMIT或ROLLBACK结束。COMMIT是提交，即提交事务的所有操作。而ROLLBACK是回滚，即事务不能继续执行，将已经完成的操作全部撤销，回滚到事务开始时的状态。

事务可以是一条SQL语句、一组SQL语句或者整个程序。事务有四个特性：**原子性**、**一致性**、**隔离性**、**持续性**。

- **原子性**：事务中的操作要么做要么不做。
- **一致性**：数据库必须从一个一致性状态变到另一个一致性状态，当数据库只包含成功事务提交的结果时称该数据库处于一致性状态。
- **隔离性**：一个事务的执行不能被其他事务干扰。
- **持续性**：也称永久性，事务一旦提交，它对数据库中的数据的改变是永久性的。

事务是**并发控制**的基本单位，保证事务的ACID特性是事务处理的重要任务，而事务的ACID特性可能遭到破坏的原因之一是多个事务对数据库的并发操作造成的。为了保证事务的隔离性和一致性，数据库管理系统需要对并发操作进行正确调度。此处我们先重点讨论事务的**隔离性**。事务中涉及的操作有： **Begin**, **Read**, **Write**, **Commit** 和 **Abort**。确保隔离最简单的方法就是在开始下一个事务之前，将当前事务中的所有操作完成，即**串行时间表**。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230709214624.png)

理想情况下，我们既希望获得与串行调度相同的结果（因为已知串行调度是正确的），又希望获得并行调度的优秀性能。基本上，我们正在寻找一个相当于串行时间表的时间表。为了使时间表等效，它们必须满足以下三个规则：

- 它们涉及相同的事务。

- 在各个事务中操作的排序方式相同。

- 每个时间表都能使数据库保持在相同的状态。


如果我们找到一个其结果相当于串行调度的调度，我们称该调度为**可串行化的**。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230709215052.png)

一个调度Sc在保证冲突操作的次序不变的情况下，通过交换两个事务不冲突操作的次序得到另一个调度Sc'，如果Sc'是串行的，称调度Sc为**冲突可串行化的调度**。若一个调度是冲突可串行化，则一定是可串行化的调度。可以用这种方法来判断一个调度是否是冲突可串行化。要使两个操作发生冲突，它们必须满足以下三个规则：

- 这些操作来自不同的事务。

- 两个操作都在同一资源上运行。

- 至少有一个操作是写操作。


![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo20230709215520.png)

## Locking

实现并发控制有很多方法，常见的有**封锁(locking)**、**时间戳(timestamp)**、**乐观控制法(optimisitc scheduler)** 和 **多版本并发控制(MVCC)**。这里详细介绍封锁locking，封锁就是事务T在对某个数据对象操作之前，先向系统发出请求，对其加锁。加锁后事务T就对该数据对象有了一定控制，在事务T释放它的锁之前，其他事务不能更新此数据对象。封锁类型又分为：

- **排他锁（写锁）**：若事务T对数据对象A加上X锁，则只允许T读取和修改A，其他任何事务都不能再对A加任何类型的锁，直到T释放A上的锁为止。
- **共享锁（读锁）**：若事务T对数据对象A加上S锁，则事务T可以读A但不能修改A，其他事务只能再对A加S锁，而不能加X锁，直到T释放A上的S锁为止。

不过上锁不意味着万事大吉，假设事务T1给数据R1上锁，事务T2给数据R2上锁。此时T1访问R2，T2访问R1。T1等待T2给R2解锁，T2又在等待T1给R1解锁。T1和T2两个事务永远不能结束，这就形成了**死锁**。在操作系统中，同时满足以下四个条件即可引起死锁：

- **互斥**：线程对于需要的资源进行互斥的访问（例如一个线程抢到锁）。

- **持有并等待**：线程持有了资源（例如已将持有的锁），同时又在等待其他资源（例如，需要获得的锁）。

- **非抢占**：线程获得的资源（例如锁），不能被抢占。

- **循环等待**：线程之间存在一个环路，环路上每个线程都额外持有一个资源，而这个资源又是下一个线程要申请的。


回到数据库系统中，预防死锁并不适合数据库系统（会造成许多事务的中止），因此数据库一般采用**诊断与解除死锁**的方法。诊断的方法如下：

- **超时法**：如果一个事务的等待时间超过了规定的时限，就认为发生了死锁（缺点是可能误判或者是时限设置太长死锁发生后不能及时发现）。

- **等待图法**：事务等待图是一个有向图G=(T,U)，T是结点的集合，每个结点表示正运行的事务；U是边的集合，每条边表示事务等待的情况。若T2等待T1，则在T1、T2之间画一条有向边，从T2指向T1。并发控制子系统周期性地（比如每隔数秒）生成事务等待图，并进行检测。如果发现图中存在回路，则表示系统中出现了死锁。

  ![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230717215654.png)


而解除死锁的方法则是选择一个处理死锁代价最小的事务，将其撤销，释放此事务所持有的所有的锁，使其他事务得以继续运行下去。

为保证并发调度的正确性，数据库管理系统的并发控制机制必须提供一定的额手段来保证调度是可串行化的，因此采用**Two Phase Locking，即两段锁协议**。所谓两段锁协议是指所有事务必须分两个阶段对数据项上锁和解锁。

- 事务在读取之前必须获取 S（共享）锁，在写入之前必须获取 X（排他）锁。
- 在释放一个封锁之后，事务不再申请和获得任何其他封锁。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230717214359.png)

不过上面的两段锁协议存在弊端，比如说它不能防止级联中止，举个例子：

T1更新资源A，然后释放A上的锁；T2读取资源A；T1中止；此时T2也应该中止，因为它读取了A中uncommitted的值。

为了解决这个问题，我们将使用**Strict Two Phase Locking**。Strict Two Phase Locking和Two Phase Locking的不同之处在于，前者会在事务完成时会一起释放所有锁。

现在我们知道了锁的用途以及锁的类型。接下来我们就要了解**Lock Manager**如何管理锁定和解锁（或获取和释放）请求以及它如何决定何时授予锁定。LM会维护一个哈希表，以被锁定资源的名称为键。每个条目都包含一个授予集（一组授予的锁/持有每个资源锁的事务）、锁类型（S 或 X 或我们尚未介绍的类型）和一个等待队列（由于与已授予的锁冲突而无法满足的锁请求队列）。参见下图：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230717220036.png)

当锁请求到达时，Lock Manager检查授予集中或等待队列中是否有任何 Xact 想要冲突锁。如果有，则请求者被放入等待队列。如果没有，则请求者被授予锁并放入授予集中。此外，Xacts 可以请求锁升级：此时具有共享锁的 Xact 可以请求升级为排他锁。Lock Manager会将此升级请求添加到队列的前面。

最后，当我们了解了锁的概念，我们还要弄清楚实际要上锁的内容。我们想要锁定包含我们想要写入的数据的元组吗？还是page？还是table？或者甚至可能是整个数据库，以便在我们处理该数据库时没有事务可以写入该数据库？我们做出的决定会根据我们所处的情况而有很大不同。这就是我们要讨论的**Lock Granularity**，即**锁的颗粒度**。让我们将数据库系统想象成下面的树：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230717220654.png)

最顶层是数据库。下一级是tables，后面是table的pages。最后，表中的records是树中的最低层级。请记住，当我们在一个节点上放置锁时，我们也隐式地锁定了它的所有子节点。因此，可以看到我们如何向数据库系统指定我们真正希望将锁放置在哪个层级。这种多颗粒度的锁允许我们在树的不同层级放置锁。我们将有以下新的lock modes：

- **IS**：意图以更细的颗粒度获取 S 锁。

- **IX**：意图以更细的颗粒度获取 X 锁。注意：两个事务可以在同一资源上放置 IX 锁，此时它们并不会直接冲突，它们可以在两个不同的子事务上放置 X 锁。

- **SIX**：像S和IX的结合。如果我们想要阻止任何其他事务修改较低层级的资源但希望允许它们读取较低层级的资源，那么这非常有用。使用SIX，我们在这个层级声明了一个共享锁；现在，没有其他事务可以对该子树中的任何内容声明排他锁（但是，它可能可以对该事务未修改的内容声明共享锁，即我们不会放置 X 锁的内容。这留给数据库系统来处理。）


lock modes之间的兼容性如下图所示：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230717221801.png)

既然有了Lock Granularity，我们也可以对前面的Two Phase Locking进行改造，获得**Multiple Granularity Locking Protocol**：

- 每个 Xact 必须从层次结构的根节点开始。

- 要获得节点上的 S 或 IS ，必须在父节点上持有 IS 或 IX。

- 要在节点上获取 X 或 IX ，必须在父节点上保留 IX 或 SIX。

- 必须按自下而上的顺序释放锁。

- 必须满足两端锁协议和兼容性规定。

- 协议是正确的，因为它相当于直接在层次结构的叶层级设置锁。


## Code

### Layers

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230719144028.png)

- **LockManager**：管理所有的锁，将每个资源视为独立的（不考虑资源的层次结构）。LockManager负责排队逻辑，必要时阻塞事务或解除阻塞，并且LockManager是事务是否拥有某个锁的唯一权威证明。如果说LockManager说T1拥有X(database)，那么T1就拥有这个锁。

- **LockContext**：在每一个LockManager上面都有一个LockContext对象集合，每个LockContext代表一个可锁定对象（可以是page，也可以是table）。LockContext对象根据层次结构连接（例如，表的 LockContext 将数据库上下文作为父对象，将其页面上下文作为子对象）。所有 LockContext 对象都共享一个 LockManager，每个上下文都会对其方法执行多粒度约束（例如，如果事务试图请求 X(table)而不请求 IX(database)，就会出现异常）。

- **LockUtil**：顾名思义，LockUtil是一个工具类，位于 LockContext 对象集合之上，负责获取数据库使用的每个 S 或 X 请求所需的所有意图锁（例如，如果请求 S(page)，该层将负责请求 IS(database)，必要时请求 IS(table)）。


### Queuing

#### LockType

一个事务中的锁有如下类型：

- **S(A)**：可以读取A以及A的所有后代。

- **X(A)**：可以读写A以及A的所有后代。

- **IS(A)**：可以请求A的所有后代的共享锁和意图共享锁。

- **IX(A)**：可以请求A的所有后代的锁。

- **SIX(A)**：可以执行S(A)或IX(A)允许它执行的任何操作，除了请求 A 的后代上的 S、IS 或 SIX锁，因为这将是多余的（这与前文提及的SIX有出入，前文的SIX可以请求A的子节点上的SIX锁，而这里不允许这样做，这是因为SIX子节点的S是多余的）。


需要实现以下方法：

- **compatible(A,B)** 检查A锁和B锁是否兼容——两个事务对于同一资源，是否能一个事务拥有A锁而另一个事务拥有B锁？例如两个事务可以在同一资源上拥有 S 锁，所以 compatible(S, S) = true，但两个事务不能在同一资源上拥有 X 锁，所以 compatible(X, X) = false。

- **canBeParentLock(A,B)** 如果资源上的A锁允许事务获取子资源上的B锁，则返回 true。例如，要在表上获得S锁，我们必须（至少）在表的父表（即数据库）上拥有 IS 锁。因此，canBeParentLock(IS, S) = true。

- **substitutable(substitute,required)** 检查是否可以用一种锁（"替代"）代替另一种锁（"必需"）。只有当拥有substitute的事务能做拥有required的事务能做的所有事情时，才会出现这种情况。换而言之，当一个事务请求所需的锁，如果我们偷偷给它替代锁，会有问题吗？例如，如果一个事务请求 X 锁，而我们悄悄给了它一个 S 锁，那么如果该事务试图写入资源，就会出现问题。因此，substitutable(S, X) = false。


#### LockManager

需要实现以下方法：

- **acquireAndRelease** 该方法以原子的形式（从用户的角度）获取一个锁并释放零个或多个锁。该方法优先于任何排队的请求（即使有队列，它也应该继续进行，如果无法继续，则将其放置在队列的前面）。
- **acquire** 该方法是LockManager的标准获取方法。它允许事务请求一个锁，如果没有队列并且该请求与现有锁兼容，则授予该请求。否则，它应该将请求放入队列（在后面）并阻止事务。我们不允许隐式锁升级，因此在事务已经拥有 S 锁的资源上请求 X 锁是无效的。
- **release** 该方法是LockManager的标准释放方法。它允许事务释放它持有的一个锁。
- **promote** 此方法允许事务显式提升/升级持有的锁。事务用更强的锁对该资源持有的锁进行替换。该方法优先于任何排队的请求（即使有队列，它也应该继续进行，如果无法继续，则将其放置在队列的前面）。要注意的是，我们不允许升级到 SIX，该类型的请求应发送至 acquireAndRelease。这是因为在 SIX 升级期间，我们可能还需要释放多余的锁，因此我们需要使用 acquireAndRelease 来处理这些升级。
- **getLockType** 这是查询LockManager的主要方式，并返回事务对特定资源的锁类型。这在上一步中已实现。

#### Queues

每当对锁的请求无法满足时（因为它与其它事务在资源上已有的锁冲突，或者因为该资源上存在锁请求队列并且该操作不具有高于队列的优先级），则应该被放置在该资源的队列上（除非另有指定，否则放在后面），并且发出请求的事务应该被阻止。每个资源队列都独立于其它队列进行处理，并且必须释放资源上的锁后再进行处理，具体方式如下：

- 考虑队列前面的请求，如果它不与资源上的任何现有的锁冲突，则应将其从队列中删除，并且：

  - 发出请求的事务应该被授予锁

  - 请求所声明应释放的所有锁均已释放

  - 发出请求的事务应该被解锁

- 应重复上一步，直到无法满足队列中的第一个请求或队列为空。


#### Synchronization

LockManager 的方法具有同步块，以确保对 LockManager 的调用是串行的并且没有调用的交错。应该确保方法中对LockManager状态的所有访问（查询和修改）都在一个同步块内，例如：

```java
// Correct, use a single synchronized block
void acquire(...) {
    synchronized (this) {
        ResourceEntry entry = getResourceEntry(name); // fetch resource entry
        // do stuff
        entry.locks.add(...); // add to list of locks
    }
}

// Incorrect, multiple synchronized blocks
void acquire(...) {
    synchronized (this) {
        ResourceEntry entry = getResourceEntry(name); // fetch resource entry
    }
    // first synchronized block ended: another call to LockManager can start here
    synchronized (this) {
        // do stuff
        entry.locks.add(...); // add to list of locks
    }
}

// Incorrect, doing work outside of the synchronized block
void acquire(...) {
    ResourceEntry entry = getResourceEntry(name); // fetch resource entry
    // do stuff
    // other calls can run while the above code runs, which means we could
    // be using outdated lock manager state
    synchronized (this) {
        entry.locks.add(...); // add to list of locks
    }
}
```

事务在被阻塞时会阻塞整个线程，这意味着不能在同步块内阻塞事务（这将阻止对 LockManager 的任何其他调用运行，直到事务被解除阻塞……但事实并非如此，因为 LockManager 是用来解锁事务的）。

要阻止事务，请在同步块内调用 Transaction#prepareBlock，然后在同步块外调用 Transaction#block。 Transaction#prepareBlock 需要位于同步块中，以避免竞争条件，即事务在离开同步块的时刻和实际阻塞的时刻之间可能会出列。

### Multigranularity

#### LockContext

LockContext类代表层次结构中的单个资源；这是所有多粒度操作（例如在获取或执行锁升级之前强制你拥有适当的意向锁）的实现位置。

需要实现以下方法：

- **acquire** 在确保满足所有多粒度约束后，此方法通过底层 LockManager 执行获取。例如，如果事务具有 IS(database)并请求X(table)，则必须抛出适当的异常（请参阅上面方法的注释）。如果事务具有SIX锁，则该事务在任何后代资源上持有 IS/S 锁都是多余的。因此，在我们的实现中，如果祖先有SIX锁，我们就禁止获取 IS/S 锁，并将其视为无效请求。

- **release** 在确保释放后仍然满足所有多粒度约束后，此方法通过底层 LockManager 执行释放。例如，如果事务具有X(table)并尝试释放 IX(database)，则必须抛出适当的异常（请参阅上面方法的注释）。

- **promote** 在确保满足所有多粒度约束后，此方法通过底层 LockManager 执行锁升级。例如，如果事务具有IS(database)并请求从S(table)升级到X(table)，则必须抛出适当的异常（请参阅上面方法的注释）。在升级到SIX（从IS/IX/S）的特殊情况下，你应该同时释放 S/IS 类型的所有后代锁，因为当持有 SIX 锁时，我们不允许在后代上拥有 IS/S 锁。如果祖先有SIX锁，你还应该禁止升级到SIX锁，因为这是多余的。在将祖先提升到SIX锁而后代持有SIX锁的情况下，这仍然允许在SIX锁下持有SIX锁。这虽然是多余的，但修复它既混乱（必须将所有后代SIX锁与IX锁交换）又毫无意义（无论如何，你仍然持有后代锁），所以我们就保持原样。

- **escalate** 此方法将锁升级到当前级别（有关更多详细信息，请参阅下文）。由于允许多个事务（在不同线程上运行）交错执行多个 LockManager 的调用，因此你必须确保仅使用对 LockManager 的一次变异调用，并且仅从 LockManager 请求有关当前事务的相关信息（因为与任何其他事务相关的信息在查询和获取时可能会发生变化）。

- **getExplicitLockType** 此方法返回当前级别显式持有的锁的类型。例如，如果事务持有 X(db)，则 dbContext.getExplicitLockType(transaction) 应返回 X，但 tableContext.getExplicitLockType(transaction) 应返回 NL（没有显式持有锁）。

- **getEffectiveLockType** 此方法返回当前级别隐式或显式持有的锁的类型。例如，如果一个事务有 X(db)：

  - dbContext.getEffectiveLockType(transaction) 应该返回 X

  - tableContext.getEffectiveLockType(transaction) 还应该返回 X（因为我们在整个数据库上显式拥有 X 锁，因此在每个表上隐式拥有 X 锁）。


由于意向锁不会隐式地将获取锁的权限授予较低级别，因此如果事务只有 SIX（database），则 tableContext.getEffectiveLockType(transaction) 应该返回 S（而不是SIX），因为该事务通过以下方式在表上隐式拥有S：SIX锁，但不是SIX锁的IX部分（仅在数据库级别可用）。显式锁类型可以是一种类型，而有效锁类型可以是不同的锁类型，特别是如果祖先有SIX锁。


LockContext 对象都共享一个底层 LockManager 对象。 parentContext方法返回当前上下文的父级（例如调用tableContext.parentContext()时返回数据库的LockContext），childContext方法返回传入名称的子锁上下文（例如tableContext.childContext(0L)返回表的第0页的LockContext）。每个资源只有一个LockContext：多次使用相同的参数调用childContext会返回相同的对象。出于性能原因，我们不会立即为表的每个页面创建LockContext。相反，我们在创建相应的 Page 对象时才创建它们。

锁升级是从许多精细锁（层次结构中较低级别的锁）升级到单个较粗略锁（较高级别的锁）的过程。例如，我们可以将事务持有的多个页锁升级为表级别的单个锁。我们通过 LockContext#escalate 执行锁升级。对此方法的调用应解释为将后代上的所有锁（这些是精细锁）升级为调用上下文升级时使用的一个锁（粗略锁）的请求。精细锁可以是意向锁和常规锁的任意组合，但我们将粗略锁限制为 S 或 X。

例如，如果我们有以下锁：IX(database)，SIX(table)，X(page 1)，X(page 2)，X(page 4)，并调用 tableContext.escalate(transaction)，我们应该将页级锁替换为包含它们的表上的单个锁：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230720095155.png)

同样，如果我们调用 dbContext.escalate(transaction)，我们应该将页级锁和表级锁替换为包含它们的数据库上的单个锁：

![](https://3248067225-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2F-MFVQnrLlCBowpNWJo1E%2Fuploads%2Fgit-blob-5ce4b3a0fe666867c554f74df85455942421c3bc%2Fproj4-escalate2%20(1)%20(1)%20(2)%20(2)%20(3)%20(3).png?alt=media&token=8427ac3e-c038-4eaf-aa4b-667865f4963f)

请注意，在这方面，升级到X锁总是“有效”：拥有粗略的 X 锁肯定包含拥有一堆更精细的锁。但是，这会带来其他复杂性：如果事务之前仅持有更精细的 S 锁，则它不会拥有持有 X 锁所需的 IX 锁，并且升级到 X 会不必要地减少允许的并发量。因此，我们要求仅升级到仍包含替换的更精细锁的最低许可的锁类型（S 或 X 之间）（因此，如果我们只有 IS/S 锁，我们应该升级到 S，而不是 X）。另请注意，由于我们仅升级到 S 或 X，因此仅具有IS(database)的事务将升级到 S(database)。虽然只有IS(database)的事务在技术上没有较低级别的锁，但在此级别保持意向锁的唯一目的是获取较低级别的普通锁，而升级的目的是避免拥有较低级别的锁。因此，我们不允许升级到意向锁 (IS/IX/SIX)。

#### LockUtil

LockContext 类为我们强制执行多粒度约束，但在我们的数据库中使用它有点麻烦：无论我们想要请求什么锁，我们都必须处理请求适当的意向锁等。为了简化将locking集成到我们的代码库中，我们定义了ensureSufficientLockHeld方法。此方法的使用方式类似于声明性语句。例如，假设我们有一些读取整个表的代码。要添加locking，我们可以这样做：

```java
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);

// any code that reads the table here
```

在ensureSufficientLockHeld行之后，我们可以假设当前事务（Transaction.getTransaction()返回的事务）有权读取tableContext表示的资源以及任何子级（所有页面）。

我们可以连续调用它几次：

```java
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);

// any code that reads the table here
```

或以任意顺序编写多个语句：

```java
LockUtil.ensureSufficientLockHeld(pageContext, LockType.S);
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(pageContext, LockType.S);

// any code that reads the table here
```

并且不应抛出任何错误，在调用结束时，我们应该能够读取所有表。

请注意，调用者并不关心事务实际拥有哪些锁：如果我们为事务提供数据库上的 X 锁，则事务确实有权读取所有表。但这不允许太多的并发性（如果与2PL一起使用，实际上会强制执行串行调度），因此我们另外规定ensureSufficientLockHeld应该授予尽可能少的额外权限：如果S锁足够，我们应该让事务获取S锁，而不是X锁，但如果事务已经有X锁，我们应该不管它（ensureSufficientLockHeld永远不应该减少事务拥有的权限，在调用之前它应该让事务至少像以前一样多）。我们建议将此方法的逻辑分为两个阶段：确保我们在祖先上拥有适当的锁，并获取资源上的锁。在某些情况下你需要promote和escalate（这些情况并不相互排斥）。

#### Two-Phase Locking

此时，你应该有一个工作系统来获取和释放数据库中不同资源的锁。在这一部分，要添加逻辑以在整个事务过程中获取和释放锁。

**Acquisition Phase**

**读取和写入**：最简单的锁定方案是根据需要简单锁定页面。由于所有对页面的读写都是通过 Page.PageBuffer 类执行的，因此仅更改这一点就足够了。修改Page.PageBuffer中的get和put方法，以实现使用尽可能最少的许可锁类型来锁定页面（并根据需要获取层次结构上的锁）。

**扫描**：如果我们知道我们将扫描表的多个页面，那么我们最好只在表页面上的许多细粒度锁的表实例上获得一个锁。修改ridIterator和recordIterator方法以便在执行扫描之前获取表上适当的锁。

**写入优化**：当我们修改页面时，我们几乎总是会先读取它（获取 IS/S 锁），然后写回对其的更新（升级到 IX/X 锁）。如果我们提前知道要修改页面，则可以直接获取 IX/X 锁来跳过获取 IS/S 锁的过程。修改以下方法来预先请求适当的锁定：

- PageDirectory#getPageWithSpace

- Table#updateRecord

- Table#deleteRecord


**Release Phase**

此时，事务应该获取执行查询所需的大量锁，但不会释放任何锁！我们将在数据库中使用严格的Two-Phase Locking，这意味着只有在事务完成时才会在清理方法中释放锁。修改Database.TransactionContextImpl的close方法，释放事务获取的所有锁。你应该使用 LockContext#release 而不是 LockManager#release。LockManager不会验证多粒度约束，但与此同时其他事务需要假设满足这些约束，因此需要维护这些约束。最后，需要注意不能按任意顺序释放锁，要考虑释放顺序。

# Recovery

在前面讲述事务的部分，我们提及了事务的四个特性，在这一部分会涉及到**持久性**和**原子性**，持久性能保证事务结果不会丢失，原子性能使数据库从一个状态到达另外一个状态，不存在中间状态。

## Policy

### Force/No Force

如果我们使用**强制策略**，持久性是一个非常简单的属性。强制策略规定，当事务结束时，在事务提交前将所有修改过的数据页都将强制存入磁盘。这将确保持久性，因为磁盘是持久的；换而言之，页面一旦进入磁盘，就会被永久保存。这种方法的缺点是性能差，我们最终会进行大量不必要的写入。相较而言， **不强制策略**更加讨喜，即只有当页面需要从缓冲池中移除时才写回磁盘。虽然这有助于减少不必要的写入，但会使持久性变得复杂，因为如果数据库在事务提交后崩溃，一些页面可能尚未写入磁盘，因为内存是易失性的，所以会从内存中丢失。为了解决这个问题，我们将在恢复期间重做某些操作。

### Steal/No-Steal

同样，使用**不偷窃策略**也很容易确保原子性。不偷窃策略规定，在事务提交之前，页面不能从内存中移除（因此也不能写入磁盘）。这可以确保数据库不会处于中间状态，因为如果事务没有完成，那么它的任何更改都不会被写入磁盘并保存下来。这种策略的问题在于，它限制了我们使用内存的方式——我们必须将每个修改过的页面保留在内存中，直到事务完成。我们更倾向于使用**偷窃策略**，即允许在事务完成前将修改的页面写入磁盘。这将使原子性的执行变得复杂，但我们可以通过在恢复过程中撤销错误操作来解决这个问题。

### Steal, No-Force

综上，我们将使用两种策略（窃取、不强制），虽然很难保证原子性和耐久性，但却能获得最佳性能。

## Write Ahead Logging

为了解决以上问题，我们使用日志来解决。日志文件是用来记录事务对数据库的更新操作的文件，一般有两种格式，以记录为单位的日志文件和以数据块为单位的日志文件。

### Update Log Record

一条UPDATE日志记录如下所示：

```
<XID, pageID, offset, length, old_data, new_data>
```

其中：

- XID: 事务 ID——告诉我们哪个事务执行了此操作

- pageID: 哪个页面被修改了

- offset: 页面上数据开始更改的位置（通常以字节为单位）

- length: 更改了多少数据（通常以字节为单位）

- old_data: 原始数据（用于undo操作，即撤销操作）

- new_data: 更新后的数据（用于redo操作，即重做操作）


### Other Log Records

我们将在日志中还使用其他一些记录类型。在整个注释中，我们将根据需要为这些日志记录添加字段。

- COMMIT：表示事务正在开始提交过程

- ABORT：表示事务正在开始中止过程

- END：表示事务已完成（通常表示已完成提交或中止）


### WAL Requirements

与普通数据页一样，日志页也需要在内存中操作，但需要写入磁盘永久保存。**Write Ahead Logging**(WAL，预写日志)对我们何时将日志写入磁盘提出了要求。简而言之，WAL是一种策略，即在实际操作刷新到磁盘或发生之前，将描述操作（如修改数据页或提交事务）的日志记刷新到磁盘。有两条规则如下：

- **日志记录必须在相应的数据页写入磁盘之前写入磁盘**，这是我们实现原子性的方法。这样做的直观原因是，如果先写入数据页，然后数据库崩溃，我们就无法执行undo操作，因为我们不知道事务执行了什么操作。

- **事务提交时，所有日志记录都必须写入磁盘**，这是我们实现持久性的方法。直觉告诉我们，我们需要持续跟踪已提交的事务执行了哪些操作。否则，我们就不知道需要重做哪些操作。将所有日志写入磁盘后，如果数据库在修改的数据页写入磁盘前崩溃，我们就能准确知道需要重做哪些操作。


### WAL Implementation

为了实现WAL，我们将在日志记录中添加一个名为 LSN 的字段，LSN 是日志序列号（Log Sequence Number）的缩写。LSN 是一个唯一的递增数字，用于表示操作的顺序（如果看到一条 LSN = 20 的日志记录，那么该操作发生在 LSN = 10 的记录之后）。在这个类中，LSN 每次增加 10，但这只是一种约定俗成的做法。我们还将在每条日志记录中添加一个 **prevLSN** 字段，用于存储同一事务中的上一次操作（这对撤销事务非常有用）。

数据库还将跟踪存储在 RAM 中的flushedLSN。flushedLSN会跟踪已刷新到磁盘的最后一条日志记录的LSN。当一个页面被刷新时，意味着该页面已被写入磁盘；通常也意味着我们应该将该页面从内存中移除，因为我们不再需要它了。flushedLSN告诉我们，在它之前的任何日志记录都不应写入磁盘，因为它们已经在那里了。日志页通常会追加到在磁盘上的前一个日志页，因此多次写入相同的日志将意味着我们存储了重复的数据，这也会破坏日志的连续性。

我们还将为每个数据页添加一段metadata，称为pageLSN。pageLSN存储了最后一次修改页面的操作的LSN。我们将利用它来帮助我们了解哪些操作实际上已被存入磁盘，哪些操作必须重做。

### Aborting a Transaction

在讨论从崩溃中恢复之前，我们先来了解一下数据库如何中止正在进行的事务。我们可能因为出现死锁而想中止事务，或者用户可能因为事务耗时过长而决定中止事务。如果某个操作违反了某些完整性约束，也可以中止事务以保证 ACID 中的C，即一致性。最后，系统崩溃也可能导致事务中止。我们需要确保在中止过程结束后，所有操作都不会被持久化到磁盘上。

我们要做的第一件事就是在日志中写入一条 ABORT 记录，以表示我们正在启动中止进程。然后，我们将从日志中该事务的最后一个操作开始，撤销事务中的每个操作，并为每个撤销的操作向日志中写入 CLR 记录。**CLR**（Compensation Log Record，补偿日志记录）是一种新型记录，表示我们正在撤销特定操作。它与 UPDATE 记录本质上是一样的（它存储了之前的状态和新状态），但它告诉我们，这次写操作是由于中止而发生的。

## Recovery Data Structures

我们将保留两张状态表，以便恢复过程更容易一些。第一个表称为事务表，存储活动事务的信息。事务表有三个字段：

- XID: 事务ID

- status: 运行、提交或中止

- **lastLSN**: 该事务最近一次操作的LSN


![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725105849.png)

我们维护的另一个表叫做脏页面表（Dirty Page Table，DPT）。DPT 会记录哪些页面是脏页（脏页意味着页面在内存中被修改过，但尚未刷新到磁盘）。这些信息将非常有用，因为它将告诉我们哪些页面有操作但还没有刷新到磁盘。DPT 只有两列：

- Page ID

- **recLSN**: 弄“脏”页面的第一个操作


![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725110146.png)

需要注意的一点是，这两个表都存储在内存中；因此，从崩溃中恢复时，必须使用日志来重建表。当然，后面会讨论一种更简便的方法（具有检查点的恢复技术）。

## Undo Logging

我们已经介绍了很多关于数据库如何写入日志以及正常运行时如何中止事务的背景信息。现在，让我们来了解一下记录日志的原因——从故障中恢复。一种可能的恢复机制是Undo Logging。请注意，Undo Logging事实上并不使用我们之前讨论过的预写日志（WAL）。此外，它在缓冲池管理方面使用了强制和偷窃机制。

Undo Logging背后更深层次的思考是，我们希望消除所有尚未提交的事务的影响，而不消除已提交的事务的影响。为此，我们建立了 4 种类型的记录：Start、Commit、Abort和Update（包含旧值）。我们还需要制定两条规则，分别涉及如何进行日志记录以及何时将脏数据页刷新到磁盘：

- **如果一个事务修改了一个数据元素，那么相应的更新日志记录必须在包含该数据元素的 dirty 页面被写入磁盘之前写入**。我们之所以要这样做，是因为我们希望确保在新值永久取代旧值之前，旧值已被记录在磁盘上。

- **如果事务提交，那么被修改的页面必须在commit record本身写入磁盘之前写入磁盘**。这条规则确保了在事务本身实际提交之前，事务所做的所有更改都已写入磁盘。这一点很重要，因为如果我们在日志中看到了提交日志记录，那么我们就会认为该事务已提交，并且不会在恢复过程中撤销其更改。请注意，这与提前写入日志不同，在这里，脏页面是在提交记录写入磁盘之前写入磁盘的。


请注意，第一条规则执行的是偷窃策略，因为脏页面会在事务提交前写入磁盘，而第二条规则执行的是强制策略。

既然已经制定了这些规则，我们就可以讨论使用Undo Logging进行恢复的问题了。当系统崩溃时，我们首先运行recovery manager。我们从头开始扫描日志，以确定每个事务是否已完成。我们根据遇到的日志记录采取的操作如下：

- COMMIT/ABORT T: 标记T已经完成

- UPDATE T, X, v: 如果T未完成，将X=v写入磁盘，否则忽略

- START T: 忽略


我们将一直扫描到遇到检查点为止。

## Redo Logging

现在，让我们来谈谈另一种基于日志的恢复形式——Redo Logging。在这里，Redo Logging实现了缓冲区管理的 "不强制、不偷窃 "策略。在Redo Logging中，我们有与Undo Logging相同类型的日志记录，唯一不同的是更新日志记录，我们不存储特定数据元素的旧值，而是存储它将要写入的新值。

Redo Logging想要实现的与Undo Logging类似，只是在恢复时，我们不是撤销所有未完成的事务，而是重做所有已提交事务的操作。与此同时，我们会保留所有未提交的事务。与Undo Logging一样，我们也要遵守一条规则。

- 如果一个事务修改了数据元素X，则更新记录和提交记录都必须先于脏数据页本身写入磁盘——这就是不偷窃策略。因此，脏数据页的写入时间晚于事务提交记录，本质上属于预写日志。

Redo Logging的恢复相当简单：我们只需从头开始读取日志，并重做已提交事务的所有更新。虽然这看似操作很多，但可以像Undo Logging一样，通过检查点进行优化。

## ARIES Recovery Algorithm

当数据库崩溃时，它唯一可以访问的就是写入到磁盘的日志和磁盘上的数据页。 根据此信息，它应该自行恢复，以便所有已提交的事务操作都持久化（持久性），并且在崩溃前未完成的所有事务都可以正确撤销（原子性）。 恢复算法由 3 个阶段组成，按以下顺序执行：

- **Analysis Phase**: 重建Xact Table和DPT

- **Redo Phase**: redo以确保持久性

- **Undo Phase**: undo崩溃时正在运行的事务中的操作以确保原子性


### Analysis Phase

分析阶段的目的是就是重建Xac 表和 DPT在数据库崩溃前的样子。 为此，我们从头开始扫描日志中的所有记录，并根据以下规则修改表格：

- 在任何不是 END 记录的记录上：将事务添加到 Xact 表（如果需要），将事务的lastLSN设置为你当前所在的记录的LSN。

- 如果记录是 COMMIT 或 ABORT 记录，则相应地更改 Xact 表中事务的状态。

- 如果该记录是一条 UPDATE 记录，该页不在 DPT 中，则将该页添加到 DPT 中，并将 recLSN 设置为LSN。

- 如果该记录是 END 记录，则从 Xact 表中删除该事务。


在分析阶段快结束时，对于任何正在提交的事务，我们还将 END 记录写入日志并从 Xact 表中删除该事务。 此外，崩溃时正在运行的任何事务都需要中止，并且记录ABORT记录。

到目前为止，分析阶段有一个关键问题，那就是它需要数据库扫描整个日志。 在实际生产环境中，这是不现实的，因为可能有数百万条记录。 为了加速分析阶段，我们将使用**检查点**。 检查点将 Xact 表和 DPT 的内容写入日志。 这样，我们就可以从最后一个检查点开始，而不是从日志的开头开始。现在我们考虑检查点的一种变体，即模糊检查点，它实际上将两条记录写入日志，一条<BEGIN_CHECKPOINT>记录表示检查点何时开始，一条<END_CHECKPOINT>记录表示我们何时完成将表写入日志。 写入日志的表可以是 <BEGIN_CHECKPOINT> 和 <END_CHECKPOINT> 之间任意点的表状态。 这意味着我们需要从 <BEGIN_CHECKPOINT> 开始，因为我们不确定其后面的记录是否实际反映在写入日志的表中。

举个例子：

假如数据库崩溃并有如下log，右侧的Xact Table和DPT均在<END_CHECKPOINT> 记录中找到。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725140527.png)

首先，我们从 LSN 60 处的记录开始，因为它是紧接在begin checkpoint记录之后的记录。这是一条 UPDATE 记录，并且 T3 已经在 Xact 表中，因此我们将更新 lastLSN，在DPT中page已经更新，因此DPT不需要修改。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725140946.png)

现在我们来到LSN 70 处的记录。它是一条ABORT 记录，因此我们需要将Xact 表中的状态更改为Aborting 并更新lastLSN。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725141339.png)

对于end checkpoint记录无需执行任何操作，因此我们移至 LSN 90 处的 CLR (UNDO)。T3 在 Xact 表中，因此我们更新lastLSN，并且它正在修改的页面 (P3) 已在 DPT 中，因此我们再次不必修改 DPT。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725141455.png)

在 LSN 100 处，我们有另一个更新操作，并且 T1 已经在 Xact 表中，因此我们将更新其lastLSN。然而，该记录正在更新的页面不在 DPT 中，因此我们将使用 100 的 recLSN 添加它，因为这是第一个脏页面的操作。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725141652.png)

接下来是 LSN 110，它是 COMMIT 记录。我们需要将T1的状态更改为committing并更新lastLSN。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725141728.png)

最后，LSN 120 是一条 END 记录，这意味着我们需要从 Xact 表中删除 T1。

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725141749.png)

请注意，在这个问题中，我们省略了结束提交事务和中止正在运行的事务的最后一步。实际上，在重做阶段开始之前，我们会将 T2 的状态更改为中止。

### Redo Phase

恢复的下一阶段是重做阶段，以确保持久性。我们将重演历史，以重建崩溃时的状态。我们从 DPT 中最小的 recLSN 开始，因为这是可能尚未写入磁盘的第一个操作。我们将重做所有 UPDATE 和 CLR 操作，除非满足以下条件之一：

- 该页面不在 DPT 中。如果该页面不在 DPT 中，则意味着所有更改（以及这一更改）都已刷新到磁盘。

- recLSN > LSN。这是因为第一次弄脏页面的更新是在此操作之后发生的。这意味着我们当前所在的操作已经写入磁盘，否则它将是recLSN。

- pageLSN(disk) > LSN。如果将其写入磁盘的页面的最新更新发生在当前操作之后，那么我们就知道当前操作一定已将其写入磁盘。


举个例子：

接着Analysis Phase后的结果

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725142243.png)

首先，我们需要从LSN10开始恢复，因为这是DPT中最小的recLSN。其次需要redo的操作如下：10，40，60，90，100。20不需要redo是因为recLSN>LSN；30不需要redo是因为P2不在DPT；50，70，80，110和120不需要redo是因为它们不是UPDATE，也不是CLR。

### Undo Phase

恢复过程的最后阶段是撤销阶段，它确保原子性。 撤销阶段将从日志末尾开始，并逐渐向日志开头延伸。 它会撤销崩溃时每个处于活跃状态（正在运行或中止）的事务的每个更新（仅更新），以确保数据库不会处于中间状态。 如果 UPDATE 已被撤销（因此 CLR 记录已存在于该 UPDATE 的日志中），那么它不会撤销 UPDATE。

对于撤销阶段撤销的每个 UPDATE，它都会将相应的 CLR 记录写入日志。 CLR 记录还有一个我们尚未引入的附加字段，称为 undoNextLSN。 undoNextLSN 存储该事务要撤销的下一个操作的 LSN（它来自于要撤销的操作的prevLSN，从后往前）。 撤销事务的所有操作后，将该事务的 END 记录写入日志。

举个例子：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725143236.png)

首先认识到提供的日志缺少分析阶段的一条记录。 请记住，在分析阶段的最后，我们需要为任何中止事务写入日志条目。 因此，在 LSN 130 处应该有一条中止 T2 的 ABORT 记录。 它的 prevLSN 为 30，因为这是 T2 在此 ABORT 操作之前执行的上一个操作。 为了完整性，我们将此记录包含在最终答案中，但请注意，从技术上讲，**它不是在撤销阶段写入的，而是在分析阶段结束时写入的**。

我们现在继续撤销 T2 和 T3 的操作。 T3 的最新更新发生在 LSN 60，但请注意日志中已存在该操作的 CLR (LSN 90)。 因为该操作已撤销，所以我们不需要再次撤销它。

下一个操作是 LSN 40 处的 UPDATE。此更新不会在日志中的其他任何位置撤销，因此我们需要撤销它并写入相应的 CLR 记录。 prevLSN 将为 90，因为该 CLR 日志记录是 T3 的上一个操作。 undoNextLSN 将为 null，因为 T3 中没有其他操作可以撤销。 由于 T3 没有更多操作可撤销，因此我们还必须写入该事务的 END 记录。

我们需要撤销的下一个操作是 T2 在 LSN 30 处的更新。由于我们之前编写了 ABORT 记录，该记录的 prevLSN 将为 130。 undoNextLSN 将再次为空，因为日志中没有对 T2 的其他操作。我们还需要为 T2 写入 END 记录，因为这是我们需要撤销的最后一个操作。这是最终答案：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725143632.png)

### Conclusion

我们现在已经介绍了整个 ARIES 恢复算法。我们首先通过重新创建事务和脏页表并重新应用未刷新的修改来重建崩溃之前的数据库状态。然后，我们中止崩溃之前正在运行的所有事务，并通过一次高效的传递撤销它们的所有影响。下面是三个阶段如何与日志记录交互以使数据库恢复一致状态的高级视图：

![](https://yeyu-1313730906.cos.ap-guangzhou.myqcloud.com/PicGo/20230725143818.png)

在这一部分，我们首先介绍了数据库如何保证即使在使用窃取、非强制策略的情况下也能从故障中恢复。以下是不同类型策略的性能和日志记录影响的摘要：

![image](https://cs186berkeley.net/notes/assets/images/12-Recovery/logging_quadrants.png)

然后，我们介绍了数据库在正确运行时如何使用预写日志记录策略来记录所有操作。我们最终介绍了数据库如何通过 3 个步骤（分析、重做、撤销）使用日志来从故障中恢复并将数据库恢复到正确的状态。

## Code

在这一部分中，要实现预写日志记录并支持保存点、回滚和符合 ACID 的重启恢复。

### Manager

该项目将以 ARIESRecoveryManager.java 为中心，它实现了 RecoveryManager 接口。回想一下，有两种不同的操作模式：**转发处理**，在数据库正常操作期间执行日志记录并维护一些元数据，例如脏页表和事务表；以及**重启恢复**（也称为崩溃恢复），其中包括数据库再次启动时执行的过程。在正常操作期间，数据库的其余部分调用recovery manager的各种方法来表示某些操作（例如页面写入或刷新）已经发生。在重启期间，将调用restart方法，使数据库恢复到有效状态。一些比较重要的类如下：

- RecoveryManager.java：概述了要实现的每个方法以及它们何时被调用。

- TransactionTableEntry.java：代表事务表中的一个条目，并跟踪诸如 LastLSN 和active savepoints之类的内容。

- LogManager.java：包含日志管理器的实现，它提供了appending、fetching和flushing日志的接口

- LogRecord.java：包含我们支持的所有不同类型日志的super class。每个日志都有一个类型和一个 LSN。 LogRecord 的某些子类可以选择支持额外的方法。

- records：该目录下包含LogRecord的所有子类。


另外一个很重要的类就是**Disk Space Manager**。虽然不会直接使用disk space manager（各种 LogRecord 子类将根据需要使用它），但它确实有助于理解我们的disk space manager如何在更高的层次组织数据。

disk space manager负责分配页，它将页划分为分区。例如，页 40000000001 是分区 4 中的第 1 页（0 索引）。分区被显式分配和释放（但当且仅当其中没有页面时才能释放），并且页面始终在分区下分配。

分区 0 保留用于存储日志，这就是为什么在某些地方会看到将分区号与 0 进行比较的检查。每个其他分区都包含一个表或一个序列化的 B+ 树对象。

### Forward Processing

当数据库正常运行时——事务正常运行，读写数据——recovery manager的工作就是维护日志，添加日志记录并确保在必要时正确刷新日志，以便我们可以随时从崩溃中恢复。

首次创建数据库时，在运行任何事务之前，recovery manager首先要做的事情是设置日志，这是在 ARIESRecoveryManager.java 中的初始化方法中完成的。 我们将主记录作为日志中的第一个日志记录存储在LSN 0处（回想一下，主记录存储最近成功检查点的开始检查点记录的LSN）。为了简化实现重启恢复的分析阶段所需的步骤，我们还需要立即执行检查点，连续写入开始和结束检查点记录，并更新主记录。

#### Transaction Status

Forward Processing期间recovery manager的部分工作是维护正在运行的事务的状态，并记录事务状态的更新。通过三种方法向recovery manager通知事务状态的变化：

- `commit`：当事务尝试进入 COMMITTING 状态时调用。

- `abort`：当事务尝试进入 ABORTING 状态时调用。

- `end`：当事务尝试进入 COMPLETE 状态时调用。


在需要实现的三个方法（commit、abort、end）中，需要使事务表保持最新，设置相应事务的状态，并将适当的日志记录写入日志（检查records/目录以了解可以创建的日志类型）。应该养成每当为事务操作添加日志时更新事务表中的lastLSN的习惯，这包括状态更改记录、更新记录和 CLR。

值得强调的是，在commit方法中，提交记录需要在commit调用返回之前刷新到磁盘以确保持久性。在end方法中，如果最后事务以中止结束，则必须在写入 EndTransaction 记录之前回滚所有修改。查看 rollbackToLSN，了解有关如何回滚的详细信息，并考虑可以将什么 LSN 传递到此方法中以完全回滚事务。

#### Logging

在正常操作期间，当某些事件发生时会调用多个方法：

- 每当有人尝试创建或删除分区或页面时，disk space manager都会调用logAllocPart、logFreePart、logAllocPage、logFreePage这些方法，并添加适当的日志记录。

- 每当有人尝试往页面写入时，缓冲区管理器就会调用 logPageWrite方法。 该方法创建并添加适当的日志记录，并相应地更新事务表和脏页表。


所有这些方法都应该使recovery manager维护的表保持最新（脏页表和事务表）。

#### Savepoints

SQL具有允许部分回滚的保存点：SAVEPOINT pomelo 为当前正在运行的事务创建一个名为 pomelo 的保存点，允许用户使用 ROLLBACK TO SAVEPOINT pomelo 回滚在保存点之后所做的所有更改，并且可以使用 RELEASE SAVEPOINT pomelo 删除保存点。

预写日志记录让我们可以实现保存点。 recovery manager有三个与保存点相关的方法，分别对应保存点的三条SQL语句，并遵循相应SQL语句的语义：

- savepoint 为当前事务创建具有指定名称的保存点。 与 SQL 中的 SAVEPOINT 语句一样，保存点的名称仅限于事务：例如两个不同的事务可能都有名为pomelo专属于自己的保存点。

- releaseSavepoint 删除当前事务的指定保存点。它的行为与 SQL 中的 RELEASE SAVEPOINT 语句相同。

- rollbackToSavepoint 将事务回滚到指定的保存点。 保存点之后所做的所有更改都应该撤销，类似于中止事务，但事务的状态不会更改。它的行为方式与 SQL 中的 ROLLBACK TO SAVEPOINT 语句相同。


#### Checkpoints

在 ARIES 中，我们定期执行模糊检查点，这些检查点甚至在其他事务正在运行时也会执行，以最大程度地减少崩溃后的恢复时间，而不会在Forward Processing期间使数据库停止。

该方法概述如下：

首先，将开始检查点记录添加到日志中。然后，我们写入结束检查点记录，考虑到由于 DPT/Xact 表条目过多，我们可能必须分解结束检查点记录。即使所有表都是空的，也应该写入一个结束检查点记录，并且只有在必要时才应该写入多个结束检查点记录。

具体实现如下：

- 遍历 dirtyPageTable 并复制条目。如果在任何时候，复制当前记录会导致结束检查点记录太大，则应将带有复制的 DPT 条目的结束检查点记录添加到日志中。

- 遍历事务表，并复制status/lastLSN，根据需要输出结束检查点记录。

- 输出一个最终结束检查点。


最后，我们必须用新的成功检查点的开始检查点记录的LSN重写主记录。

举个例子：

如果我们有 200 个 DPT 条目和 300 个事务表条目，我们将按以下顺序输出结束检查点记录：

- 具有200个DPT条目和52个事务表条目的EndCheckpoint

- 具有240个事务表条目的EndCheckpoint

- 具有8个事务表条目的EndCheckpoint


（如果一个结束检查点有 200 个 DPT 条目，则剩余空间最多可容纳 52 个表条目。单个结束检查点最多可容纳 240 个事务表条目。）

你可能会发现 EndCheckpoint.fitsInOneRecord 静态方法对此很有用，它接受两个参数：

- 记录中存储的脏页表条目数。

- 记录中存储的transaction number/status/lastLSN条目的数量并返回one page是否能装载下resulting record。


比如为了记录：

```
EndCheckpoint{
  dpt={1 => 30000, 2 => 33000, 3 => 34000},
  txnTable={1 => (RUNNING, 33000), 2 => (RUNNING, 34000)}
}
```

对应的调用是：

```java
EndCheckpoint.fitsInOneRecord(3, 2); // # of dpt entries, # of txnTable entries
```

### Restart Recovery

当数据库再次启动时，进入重启恢复。这涉及三个阶段：analysis, redo, 和undo。RecoveryManager 接口声明了一个用于重启恢复的方法：restart 方法，该方法在数据库启动时调用。

为了单独测试每个阶段，框架具有三个用于重启恢复的包私有辅助方法，需要实现它们：restartAnalysis、restartRedo 和 restartUndo，它们分别执行分析、重做和撤销阶段。

除了恢复的三个阶段之外，重启方法还做了两件事：

- 在重做和撤销阶段之间，脏页表中任何实际上不脏的页面（内存中的更改尚未刷新）都应从脏页表中删除。 如果我们不确定一个更改是否已成功刷新到磁盘，这些页面可能会作为分析阶段的结果出现在 DPT 中。

- 撤销阶段结束后，恢复就完成了。为了避免在崩溃时再次中止所有事务，我们设置了一个检查点。


#### Analysis

**Master Record**

要开始分析前，需要获取主记录，以便找到作为起点的检查点的 LSN（回想一下，在初始化时，检查点是在日志开头附近写入的，因此总是有一个作为开始的检查点）。

**Scanning the Log**

分析的目的是从日志中重建脏页表和事务表。扫描时遇到的多种类型的日志记录分为三类：事务执行操作的日志记录、检查点记录以及事务状态更改（提交/中止/结束）的日志记录（还有主记录，但在扫描日志时永远不应该出现）。

**Log Records for Transaction Operations**

这些是涉及事务的记录，因此每当遇到这些记录之一时，我们就需要更新事务表。 以下内容适用于 LogRecord#getTransNum() 中具有非空结果的任何记录：

- 如果事务不在事务表中，则应将其添加到表中（可以使用newTransaction方法创建一个Transaction对象，将其传递给startTransaction）。

- 事务的lastLSN应该被更新。


**Log Records for Page Operations**

对于某些与页面相关的日志记录，需要更新脏页表：

- UpdatePage/UndoUpdatePage 都可能弄脏内存中的页面，从而不将更改刷新到磁盘。

- FreePage/UndoAllocPage 都使它们的更改可以立即在磁盘上可见，并且可以视为将释放的页面刷新到磁盘（从 DPT 中删除页面）。

- 无需为 AllocPage/UndoFreePage 执行任何操作，如果对这种情况下如何恢复释放页面之前的数据感到好奇，我们可以通过在释放页面之前一直编写从 [old bytes] -> [zeroes] 开始的更新日志记录来解决此问题。撤销空闲页面后，撤销这些更新将恢复到旧字节 ([zeroes] -> [old_bytes])。


**Log Records for Transaction Status Changes**

这三种日志记录（CommitTransaction/AbortTransaction/EndTransaction）都会改变事务的状态。当遇到其中一条记录时，应按照上一节所述更新事务表。事务的状态还应设置为 COMMITTING、RECOVERY_ABORTING 或 COMPLETE 之一。

如果该记录是EndTransaction记录，则在设置状态之前还应清理该事务，并从事务表中删除该条目。此外，你应该将结束事务的事务编号添加到结束事务集中，这对于处理结束检查点记录非常重要。

**Checkpoint Records**

当遇到 BeginCheckpoint 记录时，无需执行任何操作。

当遇到 EndCheckpoint 记录时，该记录中存储的表应与当前内存中的表合并：

对于脏页表的检查点快照中的每个条目：

- 即使我们已经在脏页表中有一条记录，也应该始终使用检查点中页面的recLSN，因为检查点总是比我们从日志中推断出的任何内容都更准确。

对于事务表的检查点快照中的每个条目：

- 在更新事务表条目之前，请检查相应的事务是否已在结束事务中。如果是这样，则事务已经完成，并且可以忽略该条目，因为它包含的任何信息都不再相关。否则：

  - 如果我们在重建事务表时没有该事务对应的条目，则应该添加它（可以使用newTransaction函数对象创建一个Transaction对象，该对象可以传递给startTransaction）。

  - 如果检查点中事务的lastLSN大于或等于内存事务表中事务的lastLSN，则应使用检查点中事务的lastLSN。


此外，还应更新事务状态。请记住，检查点是模糊的，这意味着它们捕获开始和结束记录之间任何时间的状态。这意味着记录中存储的某些事务状态可能已经过时，例如当我们已经知道事务正在中止时，检查点可能会说事务正在运行。事务始终会以以下两种方式之一的状态中推进：

- running -> committing -> complete

- running -> aborting -> complete


仅当检查点中的状态比内存中的状态更“高级”时，你才应该更新事务的状态。举些例子：

- 如果检查点显示事务正在中止，而我们的内存表显示其正在运行，我们应该将内存中状态更新为恢复中止，因为它可能从运行转换为中止。

- 如果检查点显示事务正在运行并且我们的内存表表明其正在提交，那么我们不会更新内存表。在正常操作中，状态无法从提交更改为运行，因此检查点状态一定是过时的。


如果检查点显示正在中止，请确保设置为恢复中止而不是中止

**Ending Transactions**

此时的事务表应具有处于以下状态之一的事务：RUNNING、COMMITTING 或 RECOVERY_ABORTING。

- 所有处于 COMMITTING 状态的事务都应该结束（cleanup()，状态设置为 COMPLETE，写入结束事务记录，并从事务表中删除）。

- 所有处于 RUNNING 状态的事务都应移至 RECOVERY_ABORTING 状态，并应写入中止事务记录。

- 对于处于 RECOVERY_ABORTING 状态的事务无需执行任何操作。


#### Redo

本节仅涉及 restartRedo 方法，该方法执行重启恢复的重做过程。重做阶段从脏页表中最低的recLSN 开始。从该点开始扫描，如果记录可重做并且满足以下任一条件，我们将重做记录：

- 与分区相关的记录（AllocPart、UndoAllocPart、FreePart、UndoFreePart）。

- 分配页面的记录（AllocPage、UndoFreePage）。

- 修改页面（UpdatePage、UndoUpdatePage、UndoAllocPage、FreePage）的记录，其中包含以下所有内容：

  - 该页面位于 DPT 中。

  - 该记录的 LSN 大于或等于该页的 DPT 的 recLSN。

  - 页面本身的 pageLSN 严格小于记录的 LSN。


为了检查页面的 pageLSN，需要从缓冲区管理器中获取它。可以使用以下模板代码：

```java
Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
try {
    // Do anything that requires the page here
} finally {
    page.unpin();
}
```

缓冲区管理器总是返回一个固定页面，这就是为什么我们使用 try-finally 块来确保页面在使用完毕后始终处于unpin的状态。请注意，我们可以在这里使用虚拟lock context，从而不必担心隔离问题，因为没有其他操作可以与重做阶段同时运行。你可能会发现 Page 类的这个方法在这里很有用。请务必考虑对空日志调用 restartRedo 的情况。

#### Undo

本节仅涉及 restartUndo 方法，该方法执行重启恢复的撤销过程。在撤销阶段，我们不会因为产生大量随机 I/O 而逐一中止和撤销事务。相反，我们会重复撤销具有最高 LSN 的日志记录（需要撤销的日志记录），直到完成为止，从而只遍历日志一次。撤销阶段从每个中止事务（处于 RECOVERY_ABORTING 状态）的lastLSN 集开始。

我们重复获取这些 LSN 中最大的日志记录，并且：

- 如果记录是可撤销的，我们将 CLR 写出并撤销它。

- 如果有一条记录，则将集合中的 LSN 替换为该记录的 undoNextLSN，否则替换为 prevLSN。

- 如果上一步的 LSN 为 0，则结​​束事务，将其从集合和事务表中删除。


LogRecord 的 undo 方法实际上并不撤销更改——它只是返回补偿日志记录。要实际撤销更改，需要添加返回的CLR，然后对其调用进行重做。

## Important differences between code and theories

在理论部分阐述的 ARIES 与项目中需要执行的recovery manager的实现之间有一些重要的区别，其中大部分都是实现细节。

### Forward Processing

| 项目  | 理论  |
| --- | --- |
| Log page/partition allocations/frees | No such logging |
| End Checkpoint may have many records | End Checkpoint is one record |

- 我们记录**页面/分区**的**分配/释放**。这只是我们的disk space manager工作方式的一个怪癖，以确保它可以在崩溃后恢复到一致的状态。

- 一个检查点可能有许多 end_checkpoint 记录，而在理论部分，仅使用单个 end_checkpoint 记录。这是因为我们需要一页来容纳一条日志记录，实际上我们可能有太多事务/脏页，以至于我们无法将其全部容纳在一页中。


### Restart Recovery

| 项目  | 理论  |
| --- | --- |
| Clean up dirty page table after redoing changes | Step does not exist |
| Checkpoint after undo | Step does not exist |
| Process checkpoints upon reaching end_checkpoint record (single pass) | Load checkpoints before starting analysis (2 passes) |
| Process page/partition allocation/free records | These entries do not exist |

- 重做所有更改后，我们将清除缓冲区管理器中所有非脏页的脏页表，而在理论部分，省略了此步骤。我们希望脏页表能够反映实际上脏的页面（因为它们被删除的唯一时刻就是刷新页面时，如果已经刷新的页面不再被修改，则这种情况可能永远不会发生）。

- 我们在撤销后设置检查点，而在理论部分，省略了此步骤。这是一个相当不重要的步骤（对于正确性而言，这不是必需的——我们在撤销后已完全恢复），但出于性能原因，它是一个有用的点，也是执行检查点的一个自然点，可以避免下次崩溃时的大量工作。

- 我们对记录进行一次遍历，在到达结束检查点记录时处理检查点，而在理论部分，我们首先创建检查点的表，然后扫描日志。这两种方法是等效的——它们在分析后会产生完全相同的表，但处理 end_checkpoint 记录并在到达它们时将其信息添加到内存中的表中更简单、更高效，特别是因为我们有多个 end_checkpoint 记录。

- 在某些情况下（free page/undo alloc page），我们会从脏页表中删除页面，而在其他情况下（allocate page/undo free page），我们不需要将页面添加到脏页表中。这是因为这些操作都会立即更新磁盘上的数据。例如，在分区的末尾分配一个页面会立即增加磁盘上支持该分区的文件大小。