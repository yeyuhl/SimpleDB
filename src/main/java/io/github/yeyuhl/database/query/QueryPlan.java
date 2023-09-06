package io.github.yeyuhl.database.query;

import io.github.yeyuhl.database.TransactionContext;
import io.github.yeyuhl.database.common.PredicateOperator;
import io.github.yeyuhl.database.databox.DataBox;
import io.github.yeyuhl.database.query.expr.Expression;
import io.github.yeyuhl.database.query.join.BNLJOperator;
import io.github.yeyuhl.database.query.join.SNLJOperator;
import io.github.yeyuhl.database.table.Record;
import io.github.yeyuhl.database.table.Schema;

import java.util.*;

/**
 * QueryPlan提供了一组方法来生成简单的查询
 * 调用与SQL语法对应的方法会将信息存储在QueryPlan中，调用execute将生成并执行QueryPlan DAG
 *
 * @author yeyuhl
 * @since 2023/6/28
 */
public class QueryPlan {
    /**
     * 包含该查询的事务
     */
    private TransactionContext transaction;

    /**
     * 表示最终查询计划的查询运算符
     */
    private QueryOperator finalOperator;

    /**
     * 要输出的列的列表（SELECT 子句）
     */
    private List<String> projectColumns;

    /**
     * 用命令行版本来传递表达式以求值
     */
    private List<Expression> projectFunctions;

    /**
     * 查询中涉及的表的别名的列表（FROM 子句）
     */
    private List<String> tableNames;

    /**
     * 表示连接的对象的列表（内部连接子句）
     */
    private List<JoinPredicate> joinPredicates;

    /**
     * 从别名到表名的映射
     */
    private Map<String, String> aliases;

    /**
     * WITH子句中临时表的别名
     */
    private Map<String, String> cteAliases;

    /**
     * 表示选择谓词的对象的列表（WHERE 子句）
     */
    private List<SelectPredicate> selectPredicates;

    /**
     * A list of columns to group by（GROUP BY 子句）
     */
    private List<String> groupByColumns;

    /**
     * 要作为排序依据的列
     */
    private String sortColumn;

    /**
     * 对生成的records数量的限制（LIMIT子句）
     */
    private int limit;

    /**
     * 从查询返回行之前要跳过的行数（OFFSET LIMIT子句）
     * 通常与LIMIT一起使用，比如：LIMIT 10 OFFSET 20; 从第21行开始返回10行
     */
    private int offset;

    /**
     * 根据transaction和baseTableName创建一个新的QueryPlan
     *
     * @param transaction   包含此查询的事务
     * @param baseTableName 此查询的源表，即要查询的表
     */
    public QueryPlan(TransactionContext transaction, String baseTableName) {
        this(transaction, baseTableName, baseTableName);
    }

    /**
     * 根据transaction和baseTableName，aliasTableName创建一个新的QueryPlan
     *
     * @param transaction    包含此查询的事务
     * @param baseTableName  此查询的源表，即要查询的表
     * @param aliasTableName 此查询的源表的别名
     */
    public QueryPlan(TransactionContext transaction, String baseTableName,
                     String aliasTableName) {
        this.transaction = transaction;

        // Our tables so far just consist of the base table
        this.tableNames = new ArrayList<>();
        this.tableNames.add(aliasTableName);

        // Handle aliasing
        this.aliases = new HashMap<>();
        this.cteAliases = new HashMap<>();
        this.aliases.put(aliasTableName, baseTableName);
        this.transaction.setAliasMap(this.aliases);

        // These will be populated as the user adds projects, selects, etc...
        this.projectColumns = new ArrayList<>();
        this.projectFunctions = null;
        this.joinPredicates = new ArrayList<>();
        this.selectPredicates = new ArrayList<>();
        this.groupByColumns = new ArrayList<>();
        this.limit = -1;
        this.offset = 0;

        // This will be set after calling execute()
        this.finalOperator = null;
    }

    public QueryOperator getFinalOperator() {
        return this.finalOperator;
    }

    /**
     * @param column 列名
     * @return 包含该列的表的名字
     * @throws IllegalArgumentException 如果列名不明确（它属于 this.tableNames 中的两个或多个表），
     *                                  或者它完全未知（它不属于 this.tableNames 中的任何表）
     */
    private String resolveColumn(String column) {
        String result = null;
        for (String tableName : this.tableNames) {
            Schema s = transaction.getSchema(tableName);
            for (String fieldName : s.getFieldNames()) {
                if (fieldName.equals(column)) {
                    if (result != null) throw new RuntimeException(
                            "Ambiguous column name `" + column + " found in both `" +
                                    result + "` and `" + tableName + "`.");
                    result = tableName;
                }
            }
        }
        if (result == null)
            throw new IllegalArgumentException("Unknown column `" + column + "`");
        return result;
    }

    @Override
    public String toString() {
        // Comically large toString() function. Formats the QueryPlan attributes
        // into SQL query format.
        StringBuilder result = new StringBuilder();
        // SELECT clause
        if (this.projectColumns.size() == 0) result.append("SELECT *");
        else {
            result.append("SELECT ");
            result.append(String.join(", ", projectColumns));
        }
        // FROM clause
        String baseTable = this.tableNames.get(0);
        String alias = aliases.get(baseTable);
        if (baseTable.equals(aliases.get(baseTable)))
            result.append(String.format("\nFROM %s\n", baseTable));
        else result.append(String.format("\nFROM %s AS %s\n", baseTable, alias));
        // INNER JOIN clauses
        for (JoinPredicate predicate : this.joinPredicates)
            result.append(String.format("    %s\n", predicate));
        // WHERE clause
        if (selectPredicates.size() > 0) {
            result.append("WHERE\n");
            List<String> predicates = new ArrayList<>();
            for (SelectPredicate predicate : this.selectPredicates) {
                predicates.add(predicate.toString());
            }
            result.append("   ").append(String.join(" AND\n   ", predicates));
            result.append("\n");
        }
        // GROUP BY clause
        if (this.groupByColumns.size() > 0) {
            result.append("GROUP BY ");
            result.append(String.join(", ", groupByColumns));
            result.append("\n");
        }
        result.append(";");
        return result.toString();
    }

    // Helper Classes //////////////////////////////////////////////////////////

    /**
     * Represents a single selection predicate. Some examples:
     * table1.col = 186
     * table2.col <= 123
     * table3.col > 6
     */
    private class SelectPredicate {
        String tableName;
        String column;
        PredicateOperator operator;
        DataBox value;

        SelectPredicate(String column, PredicateOperator operator, DataBox value) {
            if (column.contains(".")) {
                this.tableName = column.split("\\.")[0];
                column = column.split("\\.")[1];
            } else this.tableName = resolveColumn(column);
            this.column = column;
            this.operator = operator;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("%s.%s %s %s", tableName, column, operator.toSymbol(), value);
        }
    }

    /**
     * Represents an equijoin in the query plan. Some examples:
     * INNER JOIN rightTable ON leftTable.leftColumn = rightTable.rightColumn
     * INNER JOIN table2 ON table2.some_id = table1.some_id
     */
    private class JoinPredicate {
        String leftTable;
        String leftColumn;
        String rightTable;
        String rightColumn;
        private String joinTable; // Just for formatting purposes

        JoinPredicate(String tableName, String leftColumn, String rightColumn) {
            if (!leftColumn.contains(".") || !rightColumn.contains(".")) {
                throw new IllegalArgumentException("Join columns must be fully qualified");
            }

            // The splitting logic below just separates the column name from the
            // table name.
            this.joinTable = tableName;
            this.leftTable = leftColumn.split("\\.")[0];
            this.leftColumn = leftColumn;
            this.rightTable = rightColumn.split("\\.")[0];
            this.rightColumn = rightColumn;
            if (!tableName.equals(rightTable) && !tableName.equals(leftTable)) {
                throw new IllegalArgumentException(String.format(
                        "`%s` is invalid. ON clause of INNER JOIN must contain the " +
                                "new table being joined.",
                        this.toString()
                ));
            }
        }

        @Override
        public String toString() {
            String unAliased = aliases.get(joinTable);
            if (unAliased.equals(joinTable)) {
                return String.format("INNER JOIN %s ON %s = %s",
                        this.joinTable, this.leftColumn, this.rightColumn);
            }
            return String.format("INNER JOIN %s AS %s ON %s = %s",
                    unAliased, this.joinTable, this.leftColumn, this.rightColumn);
        }
    }

    // Project /////////////////////////////////////////////////////////////////

    /**
     * Add a project operator to the QueryPlan with the given column names.
     *
     * @param columnNames the columns to project
     * @throws RuntimeException a set of projections have already been
     *                          specified.
     */
    public void project(String... columnNames) {
        project(Arrays.asList(columnNames));
    }

    /**
     * Add a project operator to the QueryPlan with a list of column names. Can
     * only specify one set of projections.
     *
     * @param columnNames the columns to project
     * @throws RuntimeException a set of projections have already been
     *                          specified.
     */
    public void project(List<String> columnNames) {
        if (!this.projectColumns.isEmpty()) {
            throw new RuntimeException(
                    "Cannot add more than one project operator to this query."
            );
        }
        if (columnNames.isEmpty()) {
            throw new RuntimeException("Cannot project no columns.");
        }
        this.projectColumns = new ArrayList<>(columnNames);
    }

    public void project(List<String> names, List<Expression> functions) {
        this.projectColumns = names;
        this.projectFunctions = functions;
    }

    /**
     * Sets the final operator to a project operator with the original final
     * operator as its source. Does nothing if there are no project columns.
     */
    private void addProject() {
        if (!this.projectColumns.isEmpty()) {
            if (this.finalOperator == null) throw new RuntimeException(
                    "Can't add Project onto null finalOperator."
            );
            if (this.projectFunctions == null) {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.groupByColumns
                );
            } else {
                this.finalOperator = new ProjectOperator(
                        this.finalOperator,
                        this.projectColumns,
                        this.projectFunctions,
                        this.groupByColumns
                );
            }
        }
    }

    // Sort ////////////////////////////////////////////////////////////////////

    /**
     * Add a sort operator to the query plan on the given column.
     */
    public void sort(String sortColumn) {
        if (sortColumn == null) throw new UnsupportedOperationException("Only one sort column supported");
        this.sortColumn = sortColumn;
    }

    /**
     * Sets the final operator to a sort operator if a sort was specified and
     * the final operator isn't already sorted.
     */
    private void addSort() {
        if (this.sortColumn == null) return;
        if (this.finalOperator.sortedBy().contains(sortColumn.toLowerCase())) {
            return; // already sorted
        }
        this.finalOperator = new SortOperator(
                this.transaction,
                this.finalOperator,
                this.sortColumn
        );
    }

    // Limit ///////////////////////////////////////////////////////////////////

    /**
     * Add a limit with no offset
     *
     * @param limit an upper bound on the number of records to be yielded
     */
    public void limit(int limit) {
        this.limit(limit, 0);
    }

    /**
     * Add a limit with an offset
     *
     * @param limit  an upper bound on the number of records to be yielded
     * @param offset discards this many records before yielding the first one
     */
    public void limit(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Sets the final operator to a limit operator with the original final
     * operator as its source. Does nothing if limit is negative.
     */
    private void addLimit() {
        if (this.limit >= 0) {
            this.finalOperator = new LimitOperator(
                    this.finalOperator,
                    this.limit, this.offset
            );
        }
    }

    // Select //////////////////////////////////////////////////////////////////

    /**
     * Add a select operator. Only returns columns in which the column fulfills
     * the predicate relative to value.
     *
     * @param column   the column to specify the predicate on
     * @param operator the operator of the predicate (=, <, <=, >, >=, !=)
     * @param value    the value to compare against
     */
    public void select(String column, PredicateOperator operator,
                       Object value) {
        DataBox d = DataBox.fromObject(value);
        this.selectPredicates.add(new SelectPredicate(column, operator, d));
    }

    /**
     * For each selection predicate:
     * - creates a project operator with the final operator as its source
     * - sets the current final operator to the new project operator
     */
    private void addSelectsNaive() {
        for (int i = 0; i < selectPredicates.size(); i++) {
            SelectPredicate predicate = selectPredicates.get(i);
            this.finalOperator = new SelectOperator(
                    this.finalOperator,
                    predicate.tableName + "." + predicate.column,
                    predicate.operator,
                    predicate.value
            );
        }
    }

    // Group By ////////////////////////////////////////////////////////////////

    /**
     * Set the group by columns for this query.
     *
     * @param columns the columns to group by
     */
    public void groupBy(String... columns) {
        this.groupByColumns = Arrays.asList(columns);
    }

    /**
     * Set the group by columns for this query.
     *
     * @param columns the columns to group by
     */
    public void groupBy(List<String> columns) {
        this.groupByColumns = columns;
    }

    /**
     * Sets the final operator to a GroupByOperator with the original final
     * operator as its source. Does nothing there are no group by columns.
     */
    private void addGroupBy() {
        if (this.groupByColumns.size() > 0) {
            if (this.finalOperator == null) throw new RuntimeException(
                    "Can't add GroupBy onto null finalOperator."
            );
            this.finalOperator = new GroupByOperator(
                    this.finalOperator,
                    this.transaction,
                    this.groupByColumns
            );
        }
    }

    // Join ////////////////////////////////////////////////////////////////////

    /**
     * Join the leftColumnName column of the existing query plan against the
     * rightColumnName column of tableName.
     *
     * @param tableName       the table to join against
     * @param leftColumnName  the join column in the existing QueryPlan
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String leftColumnName, String rightColumnName) {
        join(tableName, tableName, leftColumnName, rightColumnName);
    }

    /**
     * Join the leftColumnName column of the existing queryplan against the
     * rightColumnName column of tableName, aliased as aliasTableName.
     *
     * @param tableName       the table to join against
     * @param aliasTableName  alias of table to join against
     * @param leftColumnName  the join column in the existing QueryPlan
     * @param rightColumnName the join column in tableName
     */
    public void join(String tableName, String aliasTableName, String leftColumnName,
                     String rightColumnName) {
        if (this.aliases.containsKey(aliasTableName)) {
            throw new RuntimeException("table/alias " + aliasTableName + " already in use");
        }
        if (cteAliases.containsKey(tableName)) {
            tableName = cteAliases.get(tableName);
        }
        this.aliases.put(aliasTableName, tableName);
        this.joinPredicates.add(new JoinPredicate(
                aliasTableName,
                leftColumnName,
                rightColumnName
        ));
        this.tableNames.add(aliasTableName);
        this.transaction.setAliasMap(this.aliases);
    }

    /**
     * For each table in this.joinTableNames
     * - creates a sequential scan operator over the table
     * - joins the current final operator with the sequential scan
     * - sets the final operator to the join
     */
    private void addJoinsNaive() {
        int pos = 1;
        for (JoinPredicate predicate : joinPredicates) {
            this.finalOperator = new SNLJOperator(
                    finalOperator,
                    new SequentialScanOperator(
                            this.transaction,
                            tableNames.get(pos)
                    ),
                    predicate.leftColumn,
                    predicate.rightColumn,
                    this.transaction
            );
            pos++;
        }
    }

    public void addTempTableAlias(String tableName, String alias) {
        if (cteAliases.containsKey(alias)) {
            throw new UnsupportedOperationException("Duplicate alias " + alias);
        }
        cteAliases.put(alias, tableName);
        for (String k : aliases.keySet()) {
            if (aliases.get(k).toLowerCase().equals(alias.toLowerCase())) {
                aliases.put(k, tableName);
            }
        }
        this.transaction.setAliasMap(this.aliases);
    }

    // Task 5: Single Table Access Selection ///////////////////////////////////

    /**
     * 获取所有选择谓词，这些谓词在给定表的谓词中引用的列上存在索引，并且可以在索引扫描中使用谓词运算符。
     *
     * @return this.selectPredicates中合格的选择谓词的索引列表
     */
    private List<Integer> getEligibleIndexColumns(String table) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < this.selectPredicates.size(); i++) {
            SelectPredicate p = this.selectPredicates.get(i);
            // ignore if the selection predicate is for a different table
            if (!p.tableName.equals(table)) continue;
            boolean indexExists = this.transaction.indexExists(table, p.column);
            boolean canScan = p.operator != PredicateOperator.NOT_EQUALS;
            if (indexExists && canScan) result.add(i);
        }
        return result;
    }

    /**
     * 将所有符合条件的选择谓词应用于给定的源，index except处的谓词除外。
     * 这样做目的是因为可能有一个选择谓词已用于索引扫描，因此没有必要再次应用它。
     * 选择谓词表示为 this.selectPredicates 中的元素， “ except ”对应于该列表中谓词的索引。
     *
     * @param source 要应用于selections的a source operator。
     * @param except 查询时要跳过的索引。如果您不想跳过任何东西，可以使用值 -1。
     * @return a new query operator after select predicates have been applied
     */
    private QueryOperator addEligibleSelections(QueryOperator source, int except) {
        for (int i = 0; i < this.selectPredicates.size(); i++) {
            if (i == except) continue;
            SelectPredicate curr = this.selectPredicates.get(i);
            try {
                String colName = source.getSchema().matchFieldName(curr.tableName + "." + curr.column);
                source = new SelectOperator(source, colName, curr.operator, curr.value);
            } catch (RuntimeException err) {
                /* do nothing */
            }
        }
        return source;
    }

    /**
     * 找到访问给定表的最低成本的QueryOperator
     * 首先确定给定表的顺序扫描的成本，然后对于可以在该表上使用的每个索引，确定索引扫描的成本，跟踪最小成本的运算符并优先执行符合条件的选择谓词
     * <p>
     * 如果选择了索引扫描，在往下推选择(put down selects)时排除多余的选择谓词
     * 这个方法将在搜索算法的第一遍中被调用，以确定访问每个表的最有效的访问方式
     *
     * @return 一个具有扫描给定表的最低成本的QueryOperator，它可以是SequentialScanOperator或者是一个嵌套在任何可能的下推选择操作符中的IndexScanOperator
     * 注意，最小成本运算符的并列可以被任意打破
     */
    public QueryOperator minCostSingleAccess(String table) {
        // TODO(proj3_part2): implement
        QueryOperator minOp = new SequentialScanOperator(this.transaction, table);
        // 计算顺序扫描的成本
        int minCost = minOp.estimateIOCost();
        // 对于可以在该表上使用的每个索引，确定索引扫描的成本
        int index = -1;
        for (int i : getEligibleIndexColumns(table)) {
            // 跟踪最小成本的运算符并优先执行符合条件的选择谓词
            SelectPredicate selectPredicate = selectPredicates.get(i);
            QueryOperator indexScanOperator = new IndexScanOperator(transaction, table, selectPredicate.column, selectPredicate.operator, selectPredicate.value);
            int cost = indexScanOperator.estimateIOCost();
            if (cost < minCost) {
                minCost = cost;
                minOp = indexScanOperator;
                index = i;
            }
        }
        // 如果index为-1，则返回顺序扫描，不然就返回索引扫描
        minOp = addEligibleSelections(minOp, index);
        return minOp;
    }

    // Task 6: Join Selection //////////////////////////////////////////////////

    /**
     * 给定左运算符和右运算符之间的连接谓词，从 JoinOperator.JoinType 中的连接类型中查找成本最低的连接运算符。
     * 默认情况下仅考虑 SNLJ 和 BNLJ，以防止依赖于 GHJ、Sort 和 SMJ。
     * <p>
     * Reminder: Your implementation does not need to consider cartesian products
     * and does not need to keep track of interesting orders.
     *
     * @return lowest cost join QueryOperator between the input operators
     */
    private QueryOperator minCostJoinType(QueryOperator leftOp,
                                          QueryOperator rightOp,
                                          String leftColumn,
                                          String rightColumn) {
        QueryOperator bestOperator = null;
        int minimumCost = Integer.MAX_VALUE;
        List<QueryOperator> allJoins = new ArrayList<>();
        allJoins.add(new SNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        allJoins.add(new BNLJOperator(leftOp, rightOp, leftColumn, rightColumn, this.transaction));
        for (QueryOperator join : allJoins) {
            int joinCost = join.estimateIOCost();
            if (joinCost < minimumCost) {
                bestOperator = join;
                minimumCost = joinCost;
            }
        }
        return bestOperator;
    }

    /**
     * 迭代上一次搜索中的所有表集。对于每个表集，检查每个连接谓词，查看是否存在与新表的有效连接
     * 如果有，找到最小成本的连接，并返回一个从每个要连接的表名集合到其最低成本连接运算符的映射
     * <p>
     * 连接谓词存储为“this.joinPredicates”的元素。
     *
     * @param prevMap  将a set of tables映射到set of tables上的query operator，每个集合应该有pass number - 1个元素
     * @param pass1Map 每一个集合都正好包含一个表映射到一个单表访问（扫描）的query operator
     * @return 表名到join QueryOperator的映射，每组表名中的元素数量应该等于pass number
     */
    public Map<Set<String>, QueryOperator> minCostJoins(Map<Set<String>, QueryOperator> prevMap, Map<Set<String>, QueryOperator> pass1Map) {
        Map<Set<String>, QueryOperator> result = new HashMap<>();
        // TODO(proj3_part2): implement
        // 要实现的基本逻辑:
        // For each set of tables in prevMap
        //   For each join predicate listed in this.joinPredicates
        //      Get the left side and the right side of the predicate (table name and column)
        //
        //      Case 1: 集合包含左表不包含右表，使用pass1Map获取一个运算符来访问右表
        //      Case 2: 集合包含右表不包含左表，使用pass1Map获取一个运算符来访问左表
        //      Case 3: 否则，跳过这个连接谓词，继续循环
        //      使用来自case 1和2的运算符，使用minCostJoinType来计算新表（从pass1Map获取的表）和先前连接的表之间成本最低的连接，然后更新结果映射（如果需要的话）
        for (Set<String> prevSet : prevMap.keySet()) {
            for (JoinPredicate joinPredicate : this.joinPredicates) {
                Set<String> newSet = new HashSet<>(prevSet);
                Set<String> tableSet = new HashSet<>();
                QueryOperator joinOperator;
                // case 1
                if (prevSet.contains(joinPredicate.leftTable) && !prevSet.contains(joinPredicate.rightTable)) {
                    tableSet.add(joinPredicate.rightTable);
                    QueryOperator rightQuery = pass1Map.get(tableSet);
                    // 注意这里传参顺序，之所以要这样，是因为要遵守left deep plan，让连接表在左，单表在右
                    joinOperator = minCostJoinType(prevMap.get(prevSet), rightQuery, joinPredicate.leftColumn, joinPredicate.rightColumn);
                    newSet.add(joinPredicate.rightTable);
                }
                // case 2
                else if (prevSet.contains(joinPredicate.rightTable) && !prevSet.contains(joinPredicate.leftTable)) {
                    tableSet.add(joinPredicate.leftTable);
                    QueryOperator leftQuery = pass1Map.get(tableSet);
                    joinOperator = minCostJoinType(prevMap.get(prevSet), leftQuery, joinPredicate.rightColumn, joinPredicate.leftColumn);
                    newSet.add(joinPredicate.leftTable);
                }
                // case 3
                else {
                    continue;
                }
                result.put(newSet, joinOperator);
            }
        }
        return result;
    }

    // Task 7: Optimal Plan Selection //////////////////////////////////////////

    /**
     * 在给定的mapping中查找最低成本的QueryOperator。
     * 搜索算法在每次pass中都会生成一个mapping，并将一组表与访问这些表的最低成本QueryOperator相关联。
     *
     * @return a QueryOperator in the given mapping
     */
    private QueryOperator minCostOperator(Map<Set<String>, QueryOperator> map) {
        if (map.size() == 0) throw new IllegalArgumentException(
                "Can't find min cost operator over empty map"
        );
        QueryOperator minOp = null;
        int minCost = Integer.MAX_VALUE;
        for (Set<String> tables : map.keySet()) {
            QueryOperator currOp = map.get(tables);
            int currCost = currOp.estimateIOCost();
            if (currCost < minCost) {
                minOp = currOp;
                minCost = currCost;
            }
        }
        return minOp;
    }

    /**
     * 生成一个优化的 QueryPlan 基于 the System R cost-based query optimizer.
     *
     * @return 一个records的迭代器，包含了查询的结果
     */
    public Iterator<Record> execute() {
        this.transaction.setAliasMap(this.aliases);
        // TODO(proj3_part2): implement
        // Pass 1：对于每个表，找到访问该表的最低成本的QueryOperator，并构建每个表名到其最低成本运算符的映射
        // Pass i：在每一个pass中，使用前一个pass的结果和pass1的结果来找到每个表的最低成本连接，重复进行直到所有的表都被连接
        // 将最后的运算符设置为上一次pass中成本最低的那一个，添加group by，project，sort和limit运算符，返回最终运算符的迭代器
        int passNums = this.tableNames.size();
        // pass 1
        Map<Set<String>, QueryOperator> pass1Map = new HashMap<>();
        for (String table : this.tableNames) {
            Set<String> singleTableSet = new HashSet<>();
            singleTableSet.add(table);
            pass1Map.put(singleTableSet, minCostSingleAccess(table));
        }
        // pass i
        Map<Set<String>, QueryOperator> prevMap = pass1Map;
        for (int i = 2; i <= passNums; i++) {
            prevMap = minCostJoins(prevMap, pass1Map);
        }
        // 设置最后的运算符
        this.finalOperator = minCostOperator(prevMap);
        // 添加group by，project，sort和limit运算符
        addGroupBy();
        addProject();
        addSort();
        addLimit();
        return this.finalOperator.iterator();
    }

    // EXECUTE NAIVE ///////////////////////////////////////////////////////////
    // The following functions are used to generate a naive query plan. You're
    // free to look to them for guidance, but you shouldn't need to use any of
    // these methods when you implement your own execute function.

    /**
     * Given a simple query over a single table without any joins, such as:
     * SELECT * FROM table WHERE table.column >= 186;
     * <p>
     * We can take advantage of an index over table.column to perform a over
     * only values that meet the predicate. This function determines whether or
     * not there are any columns that we can perform this optimization with.
     *
     * @return -1 if no eligible select predicate is found, otherwise the index
     * of the eligible select predicate.
     */
    private int getEligibleIndexColumnNaive() {
        boolean hasGroupBy = this.groupByColumns.size() > 0;
        boolean hasJoin = this.joinPredicates.size() > 0;
        if (hasGroupBy || hasJoin) return -1;
        for (int i = 0; i < selectPredicates.size(); i++) {
            // For each selection predicate, check if we have an index on the
            // predicate's column. If the predicate operator is something
            // we can perform a scan with (=, >=, >, <=, <) then return
            // the index of the eligible predicate
            SelectPredicate predicate = selectPredicates.get(i);
            boolean hasIndex = this.transaction.indexExists(
                    this.tableNames.get(0), predicate.column
            );
            if (hasIndex && predicate.operator != PredicateOperator.NOT_EQUALS) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Generates a query plan over a single table that takes advantage of an
     * index over the column of `indexPredicate`.
     *
     * @param indexPredicate The index of the select predicate which we can use
     *                       in our index scan.
     */
    private void generateIndexPlanNaive(int indexPredicate) {
        SelectPredicate predicate = this.selectPredicates.get(indexPredicate);
        this.finalOperator = new IndexScanOperator(
                this.transaction, this.tableNames.get(0),
                predicate.column,
                predicate.operator,
                predicate.value
        );
        this.selectPredicates.remove(indexPredicate);
        this.addSelectsNaive();
        this.addProject();
    }

    /**
     * Generates a naive QueryPlan in which all joins are at the bottom of the
     * DAG followed by all select predicates, an optional group by operator, an
     * optional project operator, an optional sort operator, and an optional
     * limit operator (in that order).
     *
     * @return an iterator of records that is the result of this query
     */
    public Iterator<Record> executeNaive() {
        this.transaction.setAliasMap(this.aliases);
        int indexPredicate = this.getEligibleIndexColumnNaive();
        if (indexPredicate != -1) {
            this.generateIndexPlanNaive(indexPredicate);
        } else {
            // start off with a scan on the first table
            this.finalOperator = new SequentialScanOperator(
                    this.transaction,
                    this.tableNames.get(0)
            );

            // add joins, selects, group by's and projects to our plan
            this.addJoinsNaive();
            this.addSelectsNaive();
            this.addGroupBy();
            this.addProject();
            this.addSort();
            this.addLimit();
        }
        return this.finalOperator.iterator();
    }

}
