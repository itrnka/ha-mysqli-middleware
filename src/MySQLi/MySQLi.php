<?php
declare(strict_types = 1);

namespace ha\Middleware\RDBMS\MySQLi;

use ha\Internal\Error\ConnectionError;
use ha\Internal\Error\CountError;
use ha\Internal\Exception\AlreadyExistsException;
use ha\Internal\Exception\ForeignKeyRestrictionException;
use ha\Internal\Exception\NotFoundException;
use ha\Middleware\MiddlewareDefaultAbstract;

/**
 * Class MySQLi.
 * MySQL database driver - uses native PHP mysqli driver in background.
 *
 */
final class MySQLi extends MiddlewareDefaultAbstract
{

    /** @var  \mysqli */
    private $nativeDriver;

    /** @var bool */
    private $connected = false;

    /** @var int */
    private $totalQueries = 0;

    /**
     * Determine whether connection to database is opened.
     *
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->connected;
    }

    /**
     * Connect to database before executing query if not connected yet.
     *
     */
    private function _autoConnect(): void
    {
        if ($this->isConnected()) return;
        $this->_connect();
    }

    /**
     * Real connect to database
     *
     * @throws \Error
     */
    private function _connect(): void
    {
        $cfg = $this->configuration;
        if (!function_exists('mysqli_init')) {
            throw new \Error("Function mysqli_init does not exists@" . __METHOD__);
        }
        $this->nativeDriver = mysqli_init();
        if (!$this->nativeDriver) {
            throw new \Error("Failed to initialize mysqli@" . __METHOD__);
        }
        if (!$this->nativeDriver->options(MYSQLI_INIT_COMMAND, 'SET CHARACTER SET utf8')) {
            throw new \Error("Failed to set MYSQLI_INIT_COMMAND@" . __METHOD__);
        }
        if (!$this->nativeDriver->options(MYSQLI_OPT_CONNECT_TIMEOUT, 3)) {
            throw new \Error("Failed to set MYSQLI_OPT_CONNECT_TIMEOUT@" . __METHOD__);
        }

        $args = [];
        $args[] = $cfg->get('host');
        $args[] = $cfg->get('user');
        $args[] = $cfg->get('password');
        $args[] = $cfg->get('database');
        $args[] = $cfg->get('port', false);
        $args[] = $cfg->get('socket', false);
        if (is_null($args[4]) && is_null($args[5])) unset($args[4], $args[5]);
        if (@!call_user_func_array([$this->nativeDriver, 'real_connect'], $args)) {
            throw new ConnectionError("Failed to connect to database #" . $this->name() . "@" . __METHOD__);
        }
        if (!$this->nativeDriver->query("SET NAMES 'utf8'")) {
            throw new \Error("Can't set UTF8 for middleware #" . $this->name() . "@" . __METHOD__);
        }
        $this->connected = true;
    }

    /**
     * Returns escaped scalar or null value.
     *
     * @param int|float|string|bool|null $value
     *
     * @return string
     * @throws \TypeError
     */
    public function quoteScalarValue($value): string
    {
        $this->_autoConnect();
        if (is_null($value)) return 'NULL';
        if (is_bool($value)) $value = intval($value);
        if (!is_scalar($value)) {
            throw new \TypeError('Value is not scalar or NULL@' . __METHOD__);
        }
        $escaped = $this->nativeDriver->real_escape_string(strval($value));
        $escaped = '"' . $escaped . '"';
        return $escaped;
    }

    /**
     * Returns escaped name of column, table, database...
     *
     * Can be used also name with dot character (e.g. 'table.column').
     *
     * @param string $name
     *
     * @return string
     */
    public function quoteEntityName(string $name): string
    {
        $parts = explode('.', $name);
        return implode('.', array_map(function ($value) {
            $value = preg_replace('/\s+/', ' ', $value);
            $value = trim($value, ' `');
            if ($value === '') {
                throw new InvalidQueryError('Some name (column, table, database, alias, ...) in query is evaluated as empty string');
            }
            if ($value === '*') {
                return '*';
            }
            if (strcasecmp('MySQLi::DEFAULT_TABLE_COLUMN_VALUE', $value) === 0) {
                return 'DEFAULT';
            }
            return "`" . trim($value) . "`";
        }, $parts));
    }

    /**
     * Returns escaped names of columns, tables, databases... provided as array.
     *
     * Can be used also name with dot character (e.g. 'table.column').
     *
     * @param array $names
     *
     * @return array
     */
    public function quoteEntityNames(array $names): array
    {
        $return = [];
        foreach ($names AS $name) {
            $return[] = $this->quoteEntityName($name);
        }
        return $return;
    }

    /**
     * Returns implode of escaped names, columns, tables, databases...
     *
     * Can be used also name with dot character (e.g. 'table.column').
     *
     * @param array $names
     * @param string $implodeWith
     *
     * @return string
     */
    public function quoteAndImplodeEntityNames(array $names, string $implodeWith = ','): string
    {
        return implode($implodeWith, $this->quoteEntityNames($names));
    }

    /**
     * Returns escaped values from provided array.
     *
     * Array values can be only scalar or null.
     *
     * @param array $items
     *
     * @return array
     */
    public function quoteArrayValues(array $items): array
    {
        foreach ($items AS $key => $val) {
            $items[$key] = $this->quoteScalarValue($val);
        }
        return $items;
    }

    /**
     * Returns implode of escaped values from provided array.
     *
     * Array values can be only scalar or null.
     *
     * @param array $items
     * @param string $implodeWith
     *
     * @return string
     */
    public function quoteAndImplodeArrayValues(array $items, string $implodeWith = ','): string
    {
        return implode($implodeWith, $this->quoteArrayValues($items));
    }

    /**
     * Returns escaped array keys from provided array.
     *
     * @param array $items
     *
     * @return array
     */
    public function quoteArrayKeys(array $items): array
    {
        return $this->quoteEntityNames(array_keys($items));
    }

    /**
     * Returns escaped array keys as entity names and values as data from provided array.
     *
     * This is method to escape key-value pairs in array.
     *
     * @param array $items
     *
     * @return array
     */
    public function quoteArrayKeysAndValues(array $items): array
    {
        $ret = [];
        foreach ($items AS $key => $value) {
            $key = $this->quoteEntityName($key);
            $value = $this->quoteScalarValue($value);
            $ret[$key] = $value;
        }
        return $ret;
    }

    /**
     * Returns escaped keys from provided array imploded by string $implodeWith.
     *
     * @param array $items
     * @param string $implodeWith
     *
     * @return string
     */
    public function quoteAndImplodeArrayKeys(array $items, string $implodeWith = ','): string
    {
        return implode($implodeWith, $this->quoteArrayKeys($items));
    }

    /**
     * Returns query object as query builder instance.
     *
     * @return MySQLiQueryBuilder
     */
    public function createQuery(): MySQLiQueryBuilder
    {
        return new MySQLiQueryBuilder($this);
    }

    /**
     * Get quoted '$column IS NULL' helper.
     *
     * @param string $column
     *
     * @return string
     */
    public function buildColumnIsNullCondition(string $column): string
    {
        return $this->quoteEntityName($column) . ' IS NULL';
    }

    /**
     * Get quoted '$column IS NOT NULL' helper.
     *
     * @param string $column
     *
     * @return string
     */
    public function buildColumnIsNotNullCondition(string $column): string
    {
        return $this->quoteEntityName($column) . ' IS NOT NULL';
    }

    /**
     * Get quoted '$column IN($values[0], ...)' helper.
     *
     * @param string $column
     * @param array $values
     *
     * @return string
     */
    public function buildInSubQuery(string $column, array $values): string
    {
        return $this->quoteEntityName($column) . ' IN (' . implode(',', $this->quoteArrayValues($values)) . ')';
    }

    /**
     * Get quoted '$column NOT IN($values[0], ...)' helper.
     *
     * @param string $column
     * @param array $values
     *
     * @return string
     */
    public function buildNotInSubQuery(string $column, array $values): string
    {
        return $this->quoteEntityName($column) . ' NOT IN (' . implode(',', $this->quoteArrayValues($values)) . ')';
    }

    /**
     * Returns new MySQLiRowCollection.
     *
     * @param array $arrayOfStdClass
     *
     * @return MySQLiRowCollection
     */
    public function createCollection(array $arrayOfStdClass): MySQLiRowCollection
    {
        return new MySQLiRowCollection($arrayOfStdClass, $this);
    }

    /**
     * Execute query on database
     *
     * @param string $query
     *
     * @return MySQLiRowCollection
     * @throws AlreadyExistsException
     * @throws ForeignKeyRestrictionException
     * @throws \Error
     */
    public function query(string $query): MySQLiRowCollection
    {
        $this->_autoConnect();
        $this->totalQueries++;
        $time = microtime(true);
        $result = $this->nativeDriver->query($query);
        $time = microtime(true) - $time;

        // handle error
        if ($result === false) {
            $errMsg = "{$this->nativeDriver->error} (code {$this->nativeDriver->errno}) in middleware #" . $this->name() . ", query: {$query}";
            switch ($this->nativeDriver->errno) {
                case 1062:
                    throw new AlreadyExistsException($errMsg);
                    break;
                case 1451:
                    throw new ForeignKeyRestrictionException($errMsg);
                    break;
                default:
                    throw new \Error($errMsg);
            }
        }

        $rows = [];
        if ($result instanceof \mysqli_result) { // true is on UPDATE
            while ($row = $result->fetch_object()) {
                $rows[] = $row;
            }
        }
        $resultCollection = $this->createCollection($rows);
        $resultCollection->setAffectedRows($this->nativeDriver->affected_rows);
        $resultCollection->setLastInsertID($this->nativeDriver->insert_id);
        $resultCollection->setQueryTime($time);
        if ($result instanceof \mysqli_result) {
            $resultCollection->setSchema($result->fetch_fields());
            $resultCollection->applySchema();
            $result->close(); // free mysqli result
        }

        return $resultCollection;
    }

    /**
     * Execute 'SELECT FOUND_ROWS()' and return value as integer.
     *
     * @return int
     */
    public function getSQLCalcFoundRows(): int
    {
        return intval($this->readSingleValue('SELECT FOUND_ROWS()'));
    }

    /**
     * Read single row as stdClass.
     *
     * @param string $query
     *
     * @return \stdClass
     * @throws \ha\Internal\Error\CountError
     * @throws \ha\Internal\Exception\NotFoundException
     */
    public function readSingleRow(string $query): \stdClass
    {
        $result = $this->query($query);
        if ($result->count() !== 1) {
            throw new CountError('Result is not single row in middleware ' . $this->name() . ' on query: ' . $query);
        }
        foreach ($result AS $row) {
            return $row;
        }
        throw new NotFoundException();
    }

    /**
     * Read single value (scalar or null).
     *
     * @param string $query
     *
     * @return mixed
     * @throws \ha\Internal\Error\CountError
     * @throws \ha\Internal\Exception\NotFoundException
     */
    public function readSingleValue(string $query)
    {
        $result = (array)$this->readSingleRow($query);
        if (count($result) !== 1) {
            throw new CountError('Result is not single row field in middleware ' . $this->name() . ' on query: ' . $query);
        }
        foreach ($result AS $field) {
            return $field;
        }
        throw new NotFoundException();
    }

    /**
     * Read single value as string value.
     *
     * @param string $query
     *
     * @return string
     */
    public function readString(string $query): string
    {
        return $this->readSingleValue($query);
    }

    /**
     * Read single value as integer value.
     *
     * @param string $query
     *
     * @return int
     */
    public function readInt(string $query): int
    {
        return $this->readSingleValue($query);
    }

    /**
     * Read single value as float value.
     *
     * @param string $query
     *
     * @return float
     */
    public function readFloat(string $query): float
    {
        return $this->readSingleValue($query);
    }

    /**
     * Get native MySQLi instance.
     *
     * @return \mysqli
     */
    public function getNativeDriver(): \mysqli
    {
        return $this->nativeDriver;
    }
}