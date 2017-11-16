<?php
declare(strict_types=1);

namespace ha\Middleware\RDBMS\MySQLi;

/**
 * Class MySQLiWhereQuery.
 * Conditions group for query builder.
 */
class MySQLiWhereQuery
{

    /** @var MySQLiQueryBuilder */
    private $builder;

    /** @var array */
    private $conditions = [];

    /** @var string */
    private $conditionsJoinOperator;

    /** @var MySQLi */
    private $driver;

    /** @var MySQLiWhereQuery */
    private $parentCondition;

    /**
     * MySQLiWhereQuery constructor.
     *
     * @param \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder $builder
     * @param \ha\Middleware\RDBMS\MySQLi\MySQLi $driver
     * @param \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery|null $parentCondition
     */
    function __construct(MySQLiQueryBuilder $builder, MySQLi $driver, MySQLiWhereQuery $parentCondition = null)
    {
        $this->driver = $driver;
        $this->builder = $builder;
        $this->parentCondition = $parentCondition;
        $this->changeConditionsJoinOperator(MySQLiQueryConditions::JOIN_AND);
    }

    /**
     * Convert to string. Returns SQL query part.
     * @return string
     */
    public function __toString(): string
    {
        $list = [];
        foreach ($this->conditions AS $condition) {
            $list[] = strval($condition);
        }
        return '(' . implode(" {$this->conditionsJoinOperator} ", $list) . ')';
    }

    /**
     * Appends a condition as child and returns it.
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function addConditions(): MySQLiWhereQuery
    {
        $where = new MySQLiWhereQuery($this->builder, $this->driver, $this);
        $this->conditions[] = $where;
        return $where;
    }

    /**
     * Change operator value, which is used on joining appended conditions.
     *
     * @param string $operator
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     * @throws InvalidQueryError
     */
    public function changeConditionsJoinOperator(string $operator): MySQLiWhereQuery
    {
        foreach (MySQLiQueryConditions::JOIN_TYPES AS $type) {
            if (strcasecmp($type, $operator) === 0) {
                $this->conditionsJoinOperator = $type;
                return $this;
            }
        }
        throw new InvalidQueryError(
            'Invalid query conditions join operator, use one from this values: ' . implode(
                ',', MySQLiQueryConditions::JOIN_TYPES
            )
        );
    }

    /**
     * Returns root query object.
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiQueryBuilder
     */
    public function getBuilder(): MySQLiQueryBuilder
    {
        return $this->builder;
    }

    /**
     * Get parent MySQLiWhereQuery object.
     * This can be used only in MySQLiWhereQuery subtree (only for conditions, which have parent condition).
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     * @throws \Error
     */
    public function getParent(): MySQLiWhereQuery
    {
        if (!isset($this->parentCondition)) {
            throw new \Error('Trying to get non existing parent condition (this is root condition)');
        }
        return $this->parentCondition;
    }

    /**
     * Adds a condition '$column BETWEEN $value1 AND $value2'.
     *
     * @param string $column
     * @param int|float|string|bool $value1
     * @param int|float|string|bool $value2
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereBetween(string $column, $value1, $value2): MySQLiWhereQuery
    {
        $column = $this->driver->quoteEntityName($column);
        $val1 = $this->driver->quoteScalarValue($value1);
        $val2 = $this->driver->quoteScalarValue($value2);
        $this->conditions[] = "{$column} BETWEEN {$val1} AND {$val2}";
        return $this;
    }

    /**
     * Adds a condition '$column = $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereEq(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '=', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition 'EXISTS ($subQuerySQL)'.
     *
     * @param string $subQuerySQL
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    public function whereExists(string $subQuerySQL): MySQLiWhereQuery
    {
        $subQuery = trim($subQuerySQL);
        if ($subQuery === '') {
            throw new InvalidQueryError('Empty subquery for WHERE EXISTS');
        }
        $this->conditions[] = "EXISTS ({$subQuery}) ";
        return $this;
    }

    /**
     * Adds a condition '$column > $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereGt(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '>', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column >= $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereGte(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '>=', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column IN ($values[0], ...)'.
     *
     * @param string $column
     * @param array $values
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereIn(string $column, array $values): MySQLiWhereQuery
    {
        $this->conditions[] = $this->driver->buildInSubQuery($column, $values);;
        return $this;
    }

    /**
     * Adds a condition 'IN ($subQuerySQL)'.
     *
     * @param string $subQuerySQL
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     * @throws \ha\Middleware\RDBMS\MySQLi\InvalidQueryError
     */
    public function whereInQuery(string $subQuerySQL): MySQLiWhereQuery
    {
        $subQuery = trim($subQuerySQL);
        if ($subQuery === '') {
            throw new InvalidQueryError('Empty subquery for WHERE IN');
        }
        $this->conditions[] = "IN ({$subQuery}) ";
        return $this;
    }

    /**
     * Adds a condition '$column IS NOT NULL'.
     *
     * @param string $column
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereIsNotNull(string $column): MySQLiWhereQuery
    {
        $column = $this->driver->quoteEntityName($column);
        $this->conditions[] = "{$column} IS NOT NULL";
        return $this;
    }

    /**
     * Adds a condition '$column IS NULL'.
     *
     * @param string $column
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereIsNull(string $column): MySQLiWhereQuery
    {
        $column = $this->driver->quoteEntityName($column);
        $this->conditions[] = "{$column} IS NULL";
        return $this;
    }

    /**
     * Adds a condition '$column LIKE $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereLike(string $column, string $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, ' LIKE ', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column < $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereLt(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '<', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column <= $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereLte(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '<=', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column != $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereNotEq(string $column, $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, '!=', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column NOT IN ($values[0], ...)'.
     *
     * @param string $column
     * @param array $values
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereNotIn(string $column, array $values): MySQLiWhereQuery
    {
        $this->conditions[] = $this->driver->buildNotInSubQuery($column, $values);;
        return $this;
    }

    /**
     * Adds a condition '$column NOT LIKE $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereNotLike(string $column, string $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, ' NOT LIKE ', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column NOT REGEXP $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereNotRegexp(string $column, string $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, ' NOT REGEXP ', __FUNCTION__);
        return $this;
    }

    /**
     * Adds a condition '$column REGEXP $value'.
     *
     * @param string $column
     * @param int|float|string|bool $value
     *
     * @return \ha\Middleware\RDBMS\MySQLi\MySQLiWhereQuery
     */
    public function whereRegexp(string $column, string $value): MySQLiWhereQuery
    {
        $this->conditions[] = $this->_whereScalarCond($column, $value, ' REGEXP ', __FUNCTION__);
        return $this;
    }

    /**
     * Internal helper.
     *
     * @param string $column
     * @param $value
     * @param string $operator
     * @param string $function
     *
     * @return string
     * @throws InvalidQueryError
     */
    private function _whereScalarCond(string $column, $value, string $operator, string $function): string
    {
        $column = $this->driver->quoteEntityName($column);
        if (!is_scalar($value)) {
            throw new InvalidQueryError('Only scalar values are supported in ' . $function);
        }
        $escValue = $this->driver->quoteScalarValue($value);
        return "{$column}{$operator}{$escValue}";
    }

}