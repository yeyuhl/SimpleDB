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

回到SimpleDB中，我们需要实现以下重要类：

- **BPlusTree**：该文件包含管理 B+ 树结构的类。每个 B+ 树都将 DataBox 类型的键（表中的单个值或“单元格”）映射到 RecordId 类型的值（数据页上记录的标识符）。

- **BPlusNode**：一个B+节点表示B+树中的一个节点，包含与BPlusTree类似的get、put、delete等方法。 BPlusNode 是一个抽象类，实现为 LeafNode 或 InnerNode。

    - **LeafNode**：叶节点是没有后代的节点，它包含指向表中相关记录的键和记录 ID 对，以及指向其右兄弟节点的指针。

    - **InnerNode**：内部节点是存储指向子节点（它们本身可以是内部节点或叶节点）的键和指针（页码）的节点。

- **BPlusTreeMetadata**：index文件夹下包含的一个类，用于存储树的顺序和高度等有用信息。可以使用上面列出的所有类中可用的 this.metadata 实例变量来访问此类的实例。


此外有一些实现的注意事项：

- 一般来说，B+树是支持重复键的。但是，我们实现的 B+ 树要做到不支持重复键。每当插入重复键时，都需要抛出异常。

- 我们实现的B+树，仅假设内部节点和叶节点可以在单个page（数据页）上序列化，不用考虑多个pages的情况。

- 在SimpleDB中，delete不会重新平衡树。因此，对于d阶B+树中的所有非根叶节点，d和2d的条目之间的不变量（invariant）被打破。实际的B+树在删除后会重新平衡，但为了简单起见，我们不会在该数据库中对树重新平衡。


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

query目录包含所谓的**QueryOperator（查询运算符）**。对数据库的单一查询可以表示为这些运算符的组合。所有操作符都扩展了QueryOperator类，并实现了Iterable< Record >接口。Scan Operators（扫描运算符）从一个表中获取数据。其余的运算符接受一个或多个输入运算符，对输入进行转换或组合（例如，投射列、排序、连接），并返回一个records的集合。

### Join Operators

JoinOperator.java是所有连接运算符的基类。实现连接算法时，不应该直接处理Table对象或TransactionContext对象（除了将它们传递到需要它们的方法中）。

#### 1.Nested Loop Joins（嵌套循环连接）

嵌套循环连接（Nested Loop Join）是一种最基本的连接实现算法。它先从外部表（驱动表）中获取满足条件的数据，然后为每一行数据遍历一次内部表（被驱动表），获取所有匹配的数据。

**1）Simple Nested Loop Join (SNLJ)**

SNLJ的简单之处在于实现起来简单，对于外部表的每一个record，都要扫描一遍内部表，这无疑十分低效。

**2）Page Nested Loop Join (PNLJ)**

**3）Block Nested Loop Join (BNLJ)**

### Scan Operators

### Special Operators

### Other Operators