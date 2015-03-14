<?php

/**
 * @license https://github.com/simple-es/doctrine-dbal-bridge/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Test\Auxiliary;

use SimpleES\EventSourcing\Aggregate\Identifier\AggregateIdentifyingCapabilities;
use SimpleES\EventSourcing\Aggregate\Identifier\IdentifiesAggregate;

/**
 * @copyright Copyright (c) 2015 Future500 B.V.
 * @author    Jasper N. Brouwer <jasper@future500.nl>
 */
final class AggregateId implements IdentifiesAggregate
{
    use AggregateIdentifyingCapabilities;
}
