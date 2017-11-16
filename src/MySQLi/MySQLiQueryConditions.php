<?php
declare(strict_types = 1);

namespace ha\Middleware\RDBMS\MySQLi;

interface MySQLiQueryConditions
{
    /** AND operator value. */
    const JOIN_AND   = 'AND';

    /** OR operator value. */
    const JOIN_OR    = 'OR';

    /** List of available operators. */
    const JOIN_TYPES = [self::JOIN_AND, self::JOIN_OR];
}