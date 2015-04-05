<?php

/**
 * @license https://github.com/simple-es/doctrine-dbal-bridge/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Event\Store;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Types\Type;
use PDO;
use SimpleES\DoctrineDBALBridge\Exception\TableNameIsUnsafe;
use SimpleES\EventSourcing\Event\NameResolver\ResolvesEventNames;
use SimpleES\EventSourcing\Event\Store\StoresEvents;
use SimpleES\EventSourcing\Event\Stream\EventEnvelope;
use SimpleES\EventSourcing\Event\Stream\EventId;
use SimpleES\EventSourcing\Event\Stream\EventStream;
use SimpleES\EventSourcing\Exception\AggregateIdNotFound;
use SimpleES\EventSourcing\Identifier\Identifies;
use SimpleES\EventSourcing\Serializer\SerializesData;
use SimpleES\EventSourcing\Timestamp\Timestamp;

/**
 * @copyright Copyright (c) 2015 Future500 B.V.
 * @author    Jasper N. Brouwer <jasper@future500.nl>
 */
class DBALEventStore implements StoresEvents
{

    /**
     * @var ResolvesEventNames
     */
    private $eventNameResolver;

    /**
     * @var SerializesData
     */
    private $serializer;

    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var string
     */
    private $tableName;

    /**
     * @var Statement
     */
    private $insertStatement;

    /**
     * @var Statement
     */
    private $selectStatement;

    /**
     * @param ResolvesEventNames $eventNameResolver
     * @param SerializesData     $serializer
     * @param Connection         $connection
     * @param string             $tableName
     */
    public function __construct(
        ResolvesEventNames $eventNameResolver,
        SerializesData $serializer,
        Connection $connection,
        $tableName
    ) {
        $this->eventNameResolver = $eventNameResolver;
        $this->connection        = $connection;
        $this->serializer        = $serializer;
        $this->tableName         = $tableName;

        $this->ensureTableNameCanBeSafelyUsed();
    }

    /**
     * {@inheritdoc}
     */
    public function commit(EventStream $eventStream)
    {
        $statement = $this->prepareInsertStatement();

        /** @var EventEnvelope $envelope */
        foreach ($eventStream as $envelope) {
            $eventPayload = $this->serializer->serialize($envelope->event());
            $metadata     = $this->serializer->serialize($envelope->metadata());

            $statement->bindValue('event_id', (string) $envelope->eventId(), Type::GUID);
            $statement->bindValue('event_name', $envelope->eventName(), Type::STRING);
            $statement->bindValue('event_payload', $eventPayload, Type::TEXT);
            $statement->bindValue('aggregate_id', (string) $envelope->aggregateId(), Type::GUID);
            $statement->bindValue('aggregate_version', $envelope->aggregateVersion(), Type::INTEGER);
            $statement->bindValue('took_place_at', (string) $envelope->tookPlaceAt(), Type::STRING);
            $statement->bindValue('metadata', $metadata, Type::TEXT);

            $statement->execute();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function read(Identifies $aggregateId)
    {
        $statement = $this->prepareSelectStatement();
        $statement->bindValue('aggregate_id', (string) $aggregateId, Type::GUID);
        $statement->execute();

        $envelopes = [];

        while ($row = $statement->fetch(PDO::FETCH_ASSOC)) {
            $eventName  = $row['event_name'];
            $eventClass = $this->eventNameResolver->resolveEventClass($eventName);
            $event      = $this->serializer->deserialize($row['event_payload'], $eventClass);

            $metadata = $this->serializer->deserialize($row['metadata'], 'SimpleES\EventSourcing\Metadata\Metadata');

            $envelopes[] = new EventEnvelope(
                EventId::fromString($row['event_id']),
                $eventName,
                $event,
                $aggregateId,
                (int) $row['aggregate_version'],
                Timestamp::fromString($row['took_place_at']),
                $metadata
            );
        }

        if (!$envelopes) {
            throw AggregateIdNotFound::create($aggregateId);
        }

        return new EventStream($aggregateId, $envelopes);
    }

    /**
     * @return Statement
     */
    private function prepareInsertStatement()
    {
        if ($this->insertStatement === null) {
            $sql = <<< EOQ
INSERT INTO {$this->tableName}
(event_id, event_name, event_payload, aggregate_id, aggregate_version, took_place_at, metadata)
VALUES
(:event_id, :event_name, :event_payload, :aggregate_id, :aggregate_version, :took_place_at, :metadata)
EOQ;

            $this->insertStatement = $this->connection->prepare($sql);
        }

        return $this->insertStatement;
    }

    /**
     * @return Statement
     */
    private function prepareSelectStatement()
    {
        if ($this->selectStatement === null) {
            $sql = <<< EOQ
SELECT event_id, event_name, event_payload, aggregate_version, took_place_at, metadata
FROM {$this->tableName}
WHERE aggregate_id = :aggregate_id
ORDER BY aggregate_version ASC
EOQ;

            $this->selectStatement = $this->connection->prepare($sql);
        }

        return $this->selectStatement;
    }

    /**
     * @throws TableNameIsUnsafe
     */
    private function ensureTableNameCanBeSafelyUsed()
    {
        if (!preg_match('/^[0-9A-Za-z_]+$/', $this->tableName)) {
            throw TableNameIsUnsafe::create($this->tableName);
        }
    }
}
