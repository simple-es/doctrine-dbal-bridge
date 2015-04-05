<?php

/**
 * @license https://github.com/simple-es/doctrine-dbal-bridge/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Test\Core;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryTestCase;
use SimpleES\DoctrineDBALBridge\Event\Store\DBALEventStore;
use SimpleES\DoctrineDBALBridge\Test\Auxiliary\AggregateId;
use SimpleES\EventSourcing\Event\Stream\EventEnvelope;
use SimpleES\EventSourcing\Event\Stream\EventId;
use SimpleES\EventSourcing\Event\Stream\EventStream;
use SimpleES\EventSourcing\Identifier\Identifies;
use SimpleES\EventSourcing\Metadata\Metadata;
use SimpleES\EventSourcing\Timestamp\Timestamp;

/**
 * @copyright Copyright (c) 2015 Future500 B.V.
 * @author    Jasper N. Brouwer <jasper@future500.nl>
 */
class IntegrationTest extends MockeryTestCase
{

    /**
     * @var DBALEventStore
     */
    private $eventStore;

    /**
     * @var \Mockery\MockInterface
     */
    private $eventNameResolver;

    /**
     * @var \Mockery\MockInterface
     */
    private $serializer;

    /**
     * @var Connection
     */
    private $connection;

    public function setUp()
    {
        $this->eventNameResolver = Mockery::mock('SimpleES\EventSourcing\Event\NameResolver\ResolvesEventNames');

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->with('an_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnEventHappened');

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->with('another_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnotherEventHappened');

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->with('yet_another_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\YetAnotherEventHappened');

        $this->serializer = Mockery::mock('SimpleES\EventSourcing\Serializer\SerializesData');

        $this->serializer
            ->shouldReceive('serialize')
            ->with(Mockery::type('SimpleES\EventSourcing\Event\DomainEvent'))
            ->andReturn('{"foo": "bar"}');

        $this->serializer
            ->shouldReceive('serialize')
            ->with(Mockery::type('SimpleES\EventSourcing\Metadata\Metadata'))
            ->andReturn('{"bar": "foo"}');

        $this->serializer
            ->shouldReceive('deserialize')
            ->with('{"foo": "bar"}', Mockery::type('string'))
            ->andReturn(Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'));

        $this->serializer
            ->shouldReceive('deserialize')
            ->with('{"bar": "foo"}', 'SimpleES\EventSourcing\Metadata\Metadata')
            ->andReturn(new Metadata([]));

        $this->connection = DriverManager::getConnection(
            ['driver' => 'pdo_sqlite', 'memory' => true],
            new Configuration()
        );

        $sql = <<< EOQ
CREATE TABLE event_store (
    id INTEGER NOT NULL,
    event_id CHAR(36) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    event_payload CLOB NOT NULL,
    aggregate_id CHAR(36) NOT NULL,
    aggregate_version INTEGER NOT NULL,
    took_place_at CHAR(31) NOT NULL,
    metadata CLOB NOT NULL,
    PRIMARY KEY(id)
);
CREATE UNIQUE INDEX lookup_idx ON event_store (aggregate_id, aggregate_version);
EOQ;

        $this->connection->executeUpdate($sql);

        $this->eventStore = new DBALEventStore(
            $this->eventNameResolver,
            $this->serializer,
            $this->connection,
            'event_store'
        );
    }

    public function tearDown()
    {
        $this->assertPostConditions();

        $this->connection->close();

        $this->eventStore        = null;
        $this->eventNameResolver = null;
        $this->serializer        = null;
        $this->connection        = null;
    }

    /**
     * @test
     */
    public function itCommitsAnEventStreamAndThenReadsIt()
    {
        $id = AggregateId::fromString('08cd0c48-6560-430b-93d5-fcb5902f6ae3');

        $originalEventStream = $this->createEventStream($id);

        $this->eventStore->commit($originalEventStream);

        $readEventStream = $this->eventStore->read($id);

        $this->assertEquals($originalEventStream, $readEventStream);
    }

    /**
     * @param Identifies $aggregateId
     *
     * @return EventStream
     */
    private function createEventStream(Identifies $aggregateId)
    {
        $envelope1 = new EventEnvelope(
            EventId::fromString('d3ed9cf4-5a94-4da0-8b8b-d8fab0dea9ff'),
            'an_event_happened',
            Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            0,
            Timestamp::now(),
            new Metadata([])
        );

        $envelope2 = new EventEnvelope(
            EventId::fromString('4e560877-bf99-43bd-aa65-043eb8fb4ccd'),
            'another_event_happened',
            Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            1,
            Timestamp::now(),
            new Metadata([])
        );

        $envelope3 = new EventEnvelope(
            EventId::fromString('1eeb1a52-d54a-4ac1-9a3d-4a7c63952a09'),
            'yet_another_event_happened',
            Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            2,
            Timestamp::now(),
            new Metadata([])
        );

        $eventStream = new EventStream(
            $aggregateId,
            [$envelope1, $envelope2, $envelope3]
        );

        return $eventStream;
    }
}
