<?php
declare(strict_types=1);

namespace ha\Middleware\RDBMS\MySQLi;

/**
 * Class MySQLiQueryBuilder.
 * Query builder.
 */
class MySQLiQueryBuilder
{
    /** @var array */
    private $conditions = [];

    /** @var string */
    private $conditionsJoinOperator;

    /** @var MySQLi */
    private $driver;

    /** @var array */
    private $groupBy = [];

    /** @var array */
    private $orderBy = [];

    /** @var string */
    private $primaryTable;

    /** @var array */
    private $tables = [];

    /**
     * MySQLiQueryBuilder constructor.
     *
     * @param \ha\Middleware\RDBMS\MySQLi\MySQLi $driver
     */
    function __construct(MySQLi $driver)
    {
        $this->driver = $driver;
        $this->changeConditionsJoinOperator(MySQLiQueryConditions::JOIN_AND);
    }

    /**
     * Creates and returns new conditions group.
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function addConditions(): MySQLiWhereQuery
    {
        $where = new MySQLiWhereQuery($this, $this->driver);
        $this->conditions[] = $where;
        return $where;
    }

    /**
     * Change operator value, which is used on joining appended conditions.
     *
     * @param string $operator
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     * @throws \Error
     */
    public function changeConditionsJoinOperator(string $operator): MySQLiQueryBuilder
    {
        foreach (MySQLiQueryConditions::JOIN_TYPES AS $type) {
            if (strcasecmp($type, $operator) === 0) {
                $this->conditionsJoinOperator = $type;
                return $this;
            }
        }
        throw new InvalidQueryError(
            'Invalid query operator for conditions, use one from this values: ' . implode(
                ', ', MySQLiQueryConditions::JOIN_TYPES
            )
        );
    }

    /**
     * Creates 'CROSS JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function crossJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference($table, $alias, $columnReferences, $valueReferences, 'CROSS JOIN');
        return $this;
    }

    public function examples()
    {
        $query1 = $this->driver->createQuery()->table('category')->getInsertSQL(
            [
                'name' => 'x',
                'price' => 5.9
            ]
        );
        d($query1);
        $query2 = $this->driver->createQuery()->table('category')->getInsertSQL(
            [
                'name' => 'x',
                'price' => 5.9
            ], [
                'name' => 'new x'
            ], true, false, 'delayed'
        );
        d($query2);

        $query3 = $this->driver->createQuery()->table('category')->getUpdateSQL(
            [
                'name' => 'x',
                'price' => 5.9
            ], 0, 10, false, true, true
        );
        d($query3);

        $query = $this->driver->createQuery();
        $query->table('category', 'cat')->leftJoin(
            'x.table_b', 'b', ["a.id" => 'b.a_id', "a.otherid" => 'b.a_otherid'], ['b.x' => 7]
        )->innerJoin('innder.table', 'c', ["a.id" => 'c.a_id', "a.otherid" => 'c.a_otherid'], ['c.zzzz' => null])
            ->crossJoin('cross.table', 'd', ["a.id" => 'd.a_id', "a.otherid" => 'd.a_otherid'])->straightJoin(
                'straightJoin.table', 'e', ["a.id" => 'e.a_id', "a.otherid" => 'e.a_otherid']
            )->addConditions()->whereLte('c.gt', 5.5)->whereGte('c.gte_test', 5.6)->getBuilder()->orderByAsc('a.x')
            ->orderByAsc('a.y')->orderByDesc('b.x');
        #$query->groupBy('f.xxx')->groupBy('f.y');

        // OR WHERE
        $where2 = $query->addConditions()->whereEq('eq_test', 'asdf')->whereNotEq('not_eq_test', 'not_eq_asdf')
            ->whereGt('c.gt', 5.5)->whereGte('c.gte_test', 5.6)->whereLt('lt_test', 8)->whereLte('lte_test', 21455)
            ->whereIsNull('is_null ')->whereIsNotNull('is_not_null ')->whereIn('in_test', [5, 99, 8887])->whereNotIn(
                'not_in_test', [5, 99, 8887]
            );
        $query->changeConditionsJoinOperator('oR');

        $where2_1 = $where2->addConditions()->changeConditionsJoinOperator('oR')->whereIsNull('hovno_sub')->whereIn(
            'in_test', [5, 99, 8887]
        );

        d($query->getDeleteSQL(0, 10, false, true, true));
        d($query->getSelectSQL());
        d($query->getSelectSQL(['COUNT(*)'], false));

        ddd($query);
    }

    /**
     * Get self (when is called from children).
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function getBuilder(): MySQLiQueryBuilder
    {
        return $this;
    }

    /**
     * Build and get DELETE query.
     *
     * @param int $from Offset
     * @param int $size Limit.
     * @param bool $lowPriority Whether to append 'LOW_PRIORITY' flag.
     * @param bool $quick Whether to append 'QUICK' flag.
     * @param bool $ignore Whether to append 'IGNORE' flag.
     *
     * @return string SQL query.
     * @throws \Error
     */
    public function getDeleteSQL(int $from = 0, int $size = null, bool $lowPriority = false, bool $quick = false,
        bool $ignore = false
    ): string {
        $parts = ['DELETE'];
        if ($lowPriority) {
            $parts[] = 'LOW_PRIORITY';
        }
        if ($quick) {
            $parts[] = 'QUICK';
        }
        if ($ignore) {
            $parts[] = 'IGNORE';
        }
        $parts[] = $this->buildFromSQL();
        $parts[] = $this->buildWhereSQL();
        if ($this->buildGroupBySQL() !== '') {
            throw new InvalidQueryError('Delete query could not be have GROUP BY conditions');
        }
        // TODO having
        $parts[] = $this->buildOrderBySQL();
        if (isset($size)) {
            if ($from === 0) {
                $parts[] = "LIMIT {$size}";
            }
            else {
                $parts[] = "LIMIT {$from}, {$size}";
            }
        }
        return implode(' ', $parts);
    }

    /**
     * Build and get INSERT query for single row.
     *
     * @param array $keyValueRow Key value data to insert. Array keys are column names.
     * @param array $keyValueDataOnDuplicateKey Key value data used for 'ON DUPLICATE KEY UPDATE'. Array keys are
     *     column names.
     * @param bool $quoteKeyValueDataOnDuplicateKey Whether to quote $keyValueDataOnDuplicateKey. False is useful when
     *     are used SQL functions to update.
     * @param bool $ignore Whether to append 'IGNORE' flag.
     * @param string $priority Priority flag (one from: 'LOW_PRIORITY', 'DELAYED', 'HIGH_PRIORITY').
     *
     * @return string SQL query.
     * @throws InvalidQueryError
     */
    public function getInsertSQL(array $keyValueRow, array $keyValueDataOnDuplicateKey = [],
        bool $quoteKeyValueDataOnDuplicateKey = true, bool $ignore = false, string $priority = null
    ): string {
        $parts = ['INSERT'];
        $priorities = ['LOW_PRIORITY', 'DELAYED', 'HIGH_PRIORITY'];
        if (isset($priority)) {
            $found = false;
            foreach ($priorities AS $ref) {
                if (strcasecmp($ref, $priority) === 0) {
                    $parts[] = $ref;
                    $found = true;
                    break;
                }
            }
            if (!$found) {
                throw new InvalidQueryError('Invalid priority "' . $priority . '" found in query');
            }
            unset($found);
        }
        if ($ignore) {
            $parts[] = 'IGNORE';
        }
        if (!isset($this->primaryTable) || $this->primaryTable === '') {
            throw new InvalidQueryError('Primary table is required in query, but is not set');
        }
        $parts[] = "INTO {$this->primaryTable}";

        $isMultiRow = null;
        $columns = null;
        if (!count($keyValueRow)) {
            throw new InvalidQueryError('No rows found for insert');
        }
        foreach ($keyValueRow AS $test) {
            if (!isset($isMultiRow)) {
                if (is_array($test)) {
                    $isMultiRow = true;
                    $columns = array_keys($test);
                    if (!count($test)) {
                        throw new InvalidQueryError('No rows found for insert');
                    }
                }
                else {
                    $isMultiRow = false;
                    $columns = array_keys($keyValueRow);
                }
            }
            break;
        }
        $parts[] = '(' . $this->driver->quoteAndImplodeEntityNames($columns) . ') VALUES';
        if (!$isMultiRow) {
            $values = [$keyValueRow];
        }
        else {
            $values = $keyValueRow;
        }
        $insertData = [];
        foreach ($values AS $columns) {
            $insertData[] = '(' . $this->driver->quoteAndImplodeArrayValues($columns) . ')';
        }
        $parts[] = implode(', ', $insertData);

        if (count($keyValueDataOnDuplicateKey)) {
            $data = [];
            foreach ($keyValueDataOnDuplicateKey AS $k => $v) {
                $k = $this->driver->quoteEntityName($k);
                if ($quoteKeyValueDataOnDuplicateKey) {
                    $v = $this->driver->quoteScalarValue($v);
                }
                $data[] = "{$k}={$v}";
            }
            $parts[] = 'ON DUPLICATE KEY UPDATE ' . implode(', ', $data);
        }
        if (count($this->tables)) {
            throw new InvalidQueryError('Insert query could not be have JOIN');
        }
        if (count($this->conditions)) {
            throw new InvalidQueryError('Insert query could not be have WHERE conditions');
        }
        if (count($this->groupBy)) {
            throw new InvalidQueryError('Insert query could not be have GROUP BY conditions');
        }
        // TODO having
        if (count($this->orderBy)) {
            throw new InvalidQueryError('Insert query could not be have ORDER BY conditions');
        }
        return implode(' ', $parts);
    }

    /**
     * Build and get SELECT query.
     *
     * @param array $columnsOrFunctions List of columns or functions for 'SELECT ? FROM'
     * @param bool $quoteArguments Whether quote entered values. False is useful when are used SQL functions to update.
     * @param int $from Offset.
     * @param int $size Limit.
     *
     * @return string SQL query.
     */
    public function getSelectSQL(array $columnsOrFunctions = [], bool $quoteArguments = true, int $from = 0,
        int $size = null
    ): string {
        $parts = ['SELECT'];
        if ($quoteArguments) {
            $columnsOrFunctions = $this->driver->quoteEntityNames($columnsOrFunctions);
        }
        $columnsOrFunctions = trim(implode(', ', $columnsOrFunctions));
        if ($columnsOrFunctions === '') {
            $columnsOrFunctions = '*';
        }
        $parts[] = $columnsOrFunctions;
        $parts[] = $this->buildFromSQL();
        $parts[] = $this->buildWhereSQL();
        $parts[] = $this->buildGroupBySQL();
        // TODO having
        $parts[] = $this->buildOrderBySQL();
        if (isset($size)) {
            if ($from === 0) {
                $parts[] = "LIMIT {$size}";
            }
            else {
                $parts[] = "LIMIT {$from}, {$size}";
            }
        }
        return implode(' ', $parts);
    }

    /**
     * Build and get UPDATE query for single row.
     *
     * @param array $keyValueData Key value data to update. Array keys are column names.
     * @param int $from Offset.
     * @param int $size Limit.
     * @param bool $quoteValues Whether to quote $keyValueData. False is useful when are used SQL functions to update.
     * @param bool $lowPriority Whether to append 'LOW_PRIORITY' flag.
     * @param bool $ignore Whether to append 'IGNORE' flag.
     *
     * @return string SQL query.
     * @throws InvalidQueryError
     */
    public function getUpdateSQL(array $keyValueData, int $from = 0, int $size = null, bool $quoteValues = true,
        bool $lowPriority = false, bool $ignore = false
    ): string {
        $parts = ['UPDATE'];
        if ($lowPriority) {
            $parts[] = 'LOW_PRIORITY';
        }
        if ($ignore) {
            $parts[] = 'IGNORE';
        }
        $parts[] = $this->buildFromSQL(false);
        $parts[] = 'SET';
        $data = [];
        if (count($keyValueData) === 0) {
            throw new InvalidQueryError('Values not defined for update query');
        }
        foreach ($keyValueData AS $k => $v) {
            $k = $this->driver->quoteEntityName($k);
            if ($quoteValues) {
                $v = $this->driver->quoteScalarValue($v);
            }
            $data[] = "{$k}={$v}";
        }
        $parts[] = implode(', ', $data);
        $parts[] = $this->buildWhereSQL();
        if (count($this->groupBy)) {
            throw new InvalidQueryError('Update query could not be have GROUP BY conditions');
        }
        // TODO having
        $parts[] = $this->buildOrderBySQL();
        if (isset($size)) {
            if ($from === 0) {
                $parts[] = "LIMIT {$size}";
            }
            else {
                $parts[] = "LIMIT {$from}, {$size}";
            }
        }
        return implode(' ', $parts);
    }

    /**
     * Appends '$column' to GROUP BY conditions.
     *
     * @param string $column
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function groupBy(string $column): MySQLiQueryBuilder
    {
        $this->groupBy[] = $this->driver->quoteEntityName($column);
        return $this;
    }

    /**
     * Creates 'INNER JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function innerJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference($table, $alias, $columnReferences, $valueReferences, 'INNER JOIN');
        return $this;
    }

    /**
     * Creates 'LEFT JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function leftJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference($table, $alias, $columnReferences, $valueReferences, 'LEFT JOIN');
        return $this;
    }

    /**
     * Creates 'LEFT OUTER JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function leftOuterJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference(
            $table, $alias, $columnReferences, $valueReferences, 'LEFT OUTER JOIN'
        );
        return $this;
    }

    /**
     * Creates 'NATURAL LEFT OUTER JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function naturalLeftOuterJoin(string $table, string $alias, array $columnReferences,
        array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference(
            $table, $alias, $columnReferences, $valueReferences, 'NATURAL LEFT OUTER JOIN'
        );
        return $this;
    }

    /**
     * Creates 'NATURAL RIGHT OUTER JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function naturalRightOuterJoin(string $table, string $alias, array $columnReferences,
        array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference(
            $table, $alias, $columnReferences, $valueReferences, 'NATURAL RIGHT OUTER JOIN'
        );
        return $this;
    }

    /**
     * Appends '$column ASC' to ORDER BY conditions.
     *
     * @param string $column
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    public function orderByAsc(string $column): MySQLiQueryBuilder
    {
        $this->orderBy[] = $this->driver->quoteEntityName($column) . ' ASC';
        return $this;
    }

    /**
     * Appends '$column DESC' to ORDER BY conditions.
     *
     * @param string $column
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    public function orderByDesc(string $column): MySQLiQueryBuilder
    {
        $this->orderBy[] = $this->driver->quoteEntityName($column) . ' DESC';
        return $this;
    }

    /**
     * Creates 'RIGHT JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function rightJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference($table, $alias, $columnReferences, $valueReferences, 'RIGHT JOIN');
        return $this;
    }

    /**
     * Creates 'RIGHT OUTER JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function rightOuterJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference(
            $table, $alias, $columnReferences, $valueReferences, 'RIGHT OUTER JOIN'
        );
        return $this;
    }

    /**
     * Creates 'STRAIGHT_JOIN $table AS $alias ON ($columnReferences, $valueReferences)'.
     *
     * @param string $table SQL Table name.
     * @param string $alias Shortcut to table name.
     * @param array $columnReferences Assoc. array ['tableA.colA' => 'tableB.colB', ...] produces 'ON
     *     (tableA.colA=tableB.colB, ...)'
     * @param array $valueReferences Assoc. array ['tableA.colA' => 'value', ...] produces 'ON (tableA.colA='value',
     *     ...)'
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function straightJoin(string $table, string $alias, array $columnReferences, array $valueReferences = []
    ): MySQLiQueryBuilder {
        $this->tables[] = $this->buildJoinReference(
            $table, $alias, $columnReferences, $valueReferences, 'STRAIGHT_JOIN'
        );
        return $this;
    }

    /**
     * Set primary SQL table.
     *
     * @param string $name
     * @param string|null $alias
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    public function table(string $name, string $alias = null): MySQLiQueryBuilder
    {
        $name = $this->driver->quoteEntityName($name);
        $fullTable = $name;
        if (isset($alias)) {
            $alias = $this->driver->quoteEntityName($alias);
            $fullTable .= " AS {$alias}";
        }
        $this->primaryTable = $fullTable;
        return $this;
    }

    /**
     * Internal helper.
     *
     * @param bool $addFromString
     *
     * @return string
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    private function buildFromSQL(bool $addFromString = true): string
    {
        if (!isset($this->primaryTable)) {
            throw new InvalidQueryError('Primary table is required in query, but is not defined');
        }
        $list = '';
        if ($addFromString) {
            $list .= 'FROM ';
        }
        $list .= $this->primaryTable . ' ' . implode(" ", $this->tables);
        return $list;
    }

    /**
     * Internal helper.
     * @return string
     */
    private function buildGroupBySQL(): string
    {
        $list = implode(', ', array_map('trim', $this->groupBy));
        if ($list !== '') {
            $list = 'GROUP BY ' . $list;
        }
        return $list;
    }

    /**
     * Internal helper.
     *
     * @param string $table
     * @param string $alias
     * @param array $columnReferences
     * @param array $valueReferences
     * @param string $joinType
     *
     * @return string
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    private function buildJoinReference(string $table, string $alias, array $columnReferences, array $valueReferences,
        string $joinType
    ): string {
        $table = $this->driver->quoteEntityName($table);
        $alias = $this->driver->quoteEntityName($alias);
        $cols = [];
        foreach ($columnReferences AS $colA => $colB) {
            $cols[] = $this->driver->quoteEntityName($colA) . '=' . $this->driver->quoteEntityName($colB);
        }
        foreach ($valueReferences AS $col => $val) {
            if (is_null($val)) {
                $cols[] = $this->driver->quoteEntityName($col) . ' IS NULL';
                continue;
            }
            $cols[] = $this->driver->quoteEntityName($col) . '=' . $this->driver->quoteScalarValue($val);
        }
        if (count($cols) === 0) {
            throw new InvalidQueryError('Join references ON (...) not found in some query join "' . $joinType . '"');
        }
        return "{$joinType} {$table} AS {$alias} ON (" . implode(',', $cols) . ")";
    }

    /**
     * Internal helper.
     * @return string
     */
    private function buildOrderBySQL(): string
    {
        $list = implode(', ', array_map('trim', $this->orderBy));
        if ($list !== '') {
            $list = 'ORDER BY ' . $list;
        }
        return $list;
    }

    /**
     * Internal helper.
     * @return string
     */
    private function buildWhereSQL(): string
    {
        $list = [];
        foreach ($this->conditions AS $condition) {
            $list[] = strval($condition);
        }
        $list = trim(implode(" {$this->conditionsJoinOperator} ", $list));
        if ($list !== '') {
            $list = 'WHERE ' . $list;
        }
        return $list;
    }

}