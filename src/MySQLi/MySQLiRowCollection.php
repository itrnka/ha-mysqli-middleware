<?php
declare(strict_types = 1);

namespace ha\Middleware\RDBMS\MySQLi;

use ha\Internal\DefaultClass\Model\ModelCollectionDefaultAbstract;

/**
 * Class MySQLiRowCollection.
 * Result collection provided by MySQLi driver.
 *
 */
class MySQLiRowCollection extends ModelCollectionDefaultAbstract
{
    /** @var string Allow appending only instances with  this className */
    protected $allowOnly = \stdClass::class;

    /** @var string */
    protected $providerName = 'MySQLi_middleware';

    /** @var int */
    protected $affectedRows = 0;

    /** @var int */
    protected $lastInsertID = 0;

    /** @var float */
    protected $queryTime = 0.0;

    /** @var  array */
    protected $schema;

    /** @var MySQLi */
    private $driver;

    /**
     * MySQLiRowCollection constructor.
     *
     * @param array $array
     * @param MySQLi $driver
     */
    public function __construct(array $array = [], MySQLi $driver)
    {
        $this->driver = $driver;
        parent::__construct($array, $driver->name());
    }

    /**
     * Get driver instance
     *
     * @return MySQLi
     */
    public function driver() : MySQLi
    {
        return $this->driver;
    }

    /**
     * Create new self instance.
     *
     * @param array $array
     *
     * @return $this
     */
    public function newSelf(array $array = [])
    {
        return new self($array, $this->driver());
    }

    /**
     * Change property name to another name.
     *
     * @param string $originalName
     * @param string $newName
     *
     * @return MySQLiRowCollection
     */
    public function modifyItemPropertyName(string $originalName, string $newName) : MySQLiRowCollection
    {
        foreach ($this AS $obj) {
            $obj->$newName = $obj->$originalName;
            unset($obj->$originalName);
        }
        return $this;
    }

    /**
     * Remove property from every item collection.
     *
     * @param string $propertyName
     *
     * @return MySQLiRowCollection
     */
    public function removeItemProperty(string $propertyName) : MySQLiRowCollection
    {
        foreach ($this AS $obj) {
            unset($obj->$propertyName);
        }
        return $this;
    }

    /**
     * Escape and add quotes to every item property.
     *
     * @return MySQLiRowCollection
     */
    public function quoteItemProperties() : MySQLiRowCollection
    {
        foreach ($this AS $obj) {
            foreach ($obj AS $propertyName => $value)
            $obj->$propertyName = $this->driver()->quoteScalarValue($value);
        }
        return $this;
    }

    /**
     * @return int
     */
    public function getLastInsertID() : int
    {
        return $this->lastInsertID;
    }

    /**
     * @param int $lastInsertID
     *
     * @return MySQLiRowCollection
     */
    public function setLastInsertID(int $lastInsertID): MySQLiRowCollection
    {
        $this->lastInsertID = $lastInsertID;
        return $this;
    }

    /**
     * @return float
     */
    public function getQueryTime() : float
    {
        return $this->queryTime;
    }

    /**
     * @param float $queryTime
     *
     * @return MySQLiRowCollection
     */
    public function setQueryTime(float $queryTime): MySQLiRowCollection
    {
        $this->queryTime = $queryTime;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getSchema()
    {
        return $this->schema;
    }

    /**
     * @param mixed $schema
     *
     * @return MySQLiRowCollection
     */
    public function setSchema(array $schema = null)
    {
        $this->schema = $schema;
        return $this;
    }

    /**
     * Special clone method (standard __clone not works).
     *
     * @return MySQLiRowCollection
     */
    public function clone()
    {
        $array = [];
        foreach ($this AS $key=>$val) {
            $array[$key] = clone $val;
        }
        $new = new self($array, $this->driver());
        $new->setLastInsertID($this->getLastInsertID());
        $new->setSchema($this->getSchema());
        $new->setQueryTime($this->getQueryTime());
        $new->setAffectedRows($this->getAffectedRows());
        return $new;
    }

    /**
     * Apply schema to result data. Method is called by driver on data load.
     *
     * @return MySQLiRowCollection
     */
    public function applySchema() : MySQLiRowCollection
    {
        $schema = $this->getSchema();
        if (is_array($schema)) {
            foreach ($schema AS $fieldMap) {
                switch ($fieldMap->type) {
                    case MYSQLI_TYPE_DECIMAL:
                        $this->modifyItemPropertyValue($fieldMap->name, 'floatval');
                        break;
                    case MYSQLI_TYPE_NEWDECIMAL:
                        $this->modifyItemPropertyValue($fieldMap->name, 'floatval');
                        break;
                    case MYSQLI_TYPE_BIT: // Field is defined as BIT (MySQL 5.0.3 and up)
                        break;
                    case MYSQLI_TYPE_TINY: // Field is defined as TINYINT
                        $this->modifyItemPropertyValue($fieldMap->name, 'intval');
                        break;
                    case MYSQLI_TYPE_SHORT: // Field is defined as SMALLINT
                        $this->modifyItemPropertyValue($fieldMap->name, 'intval');
                        break;
                    case MYSQLI_TYPE_LONG: // Field is defined as INT
                        $this->modifyItemPropertyValue($fieldMap->name, 'intval');
                        break;
                    case MYSQLI_TYPE_FLOAT: // Field is defined as FLOAT
                        $this->modifyItemPropertyValue($fieldMap->name, 'floatval');
                        break;
                    case MYSQLI_TYPE_DOUBLE: // Field is defined as DOUBLE
                        $this->modifyItemPropertyValue($fieldMap->name, 'floatval');
                        break;
                    case MYSQLI_TYPE_NULL: // Field is defined as DEFAULT NULL
                        break;
                    case MYSQLI_TYPE_TIMESTAMP: // Field is defined as TIMESTAMP
                        break;
                    case MYSQLI_TYPE_LONGLONG: // Field is defined as BIGINT
                        break;
                    case MYSQLI_TYPE_INT24: // Field is defined as MEDIUMINT
                        $this->modifyItemPropertyValue($fieldMap->name, 'intval');
                        break;
                    case MYSQLI_TYPE_DATE: // Field is defined as DATE
                        break;
                    case MYSQLI_TYPE_TIME: // Field is defined as TIME
                        break;
                    case MYSQLI_TYPE_DATETIME: // Field is defined as DATETIME
                        break;
                    case MYSQLI_TYPE_YEAR: // Field is defined as YEAR
                        break;
                    case MYSQLI_TYPE_NEWDATE: // Field is defined as DATE
                        break;
                    case MYSQLI_TYPE_INTERVAL: // Field is defined as INTERVAL
                        break;
                    case MYSQLI_TYPE_ENUM: // Field is defined as ENUM
                        break;
                    case MYSQLI_TYPE_SET: // Field is defined as SET
                        break;
                    case MYSQLI_TYPE_TINY_BLOB: // Field is defined as TINYBLOB
                        break;
                    case MYSQLI_TYPE_MEDIUM_BLOB: // Field is defined as MEDIUMBLOB
                        break;
                    case MYSQLI_TYPE_LONG_BLOB: // Field is defined as LONGBLOB
                        break;
                    case MYSQLI_TYPE_BLOB: // Field is defined as BLOB
                        break;
                    case MYSQLI_TYPE_VAR_STRING: // Field is defined as VARCHAR
                        break;
                    case MYSQLI_TYPE_STRING: // Field is defined as CHAR or BINARY
                        break;
                    case MYSQLI_TYPE_CHAR: // Field is defined as TINYINT. For CHAR, see MYSQLI_TYPE_STRING
                        $this->modifyItemPropertyValue($fieldMap->name, 'intval');
                        break;
                    case MYSQLI_TYPE_GEOMETRY: // Field is defined as GEOMETRY
                        break;
                    default:
                }
            }
        }
        return $this;
    }

}