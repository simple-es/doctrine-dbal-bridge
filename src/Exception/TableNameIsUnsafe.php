<?php

/**
 * @license https://github.com/simple-es/event-sourcing/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Exception;

use SimpleES\EventSourcing\Exception\Exception;

/**
 * @copyright Copyright (c) 2015 Future500 B.V.
 * @author    Jasper N. Brouwer <jasper@future500.nl>
 */
final class TableNameIsUnsafe extends \InvalidArgumentException implements Exception
{
    /**
     * @param string $tableName
     * @return TableNameIsUnsafe
     */
    public static function create($tableName)
    {
        return new TableNameIsUnsafe(
            sprintf('Table name %s is unsafe', $tableName)
        );
    }
}
