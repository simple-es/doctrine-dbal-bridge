<?php

/**
 * @license https://github.com/simple-es/doctrine-dbal-bridge/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Test\Core;

use Doctrine\DBAL\Types\Type;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryTestCase;
use PDO;
use SimpleES\DoctrineDBALBridge\Event\Store\DBALEventStore;
use SimpleES\DoctrineDBALBridge\Test\Auxiliary\AggregateId;
use SimpleES\EventSourcing\Event\Stream\EventId;
use SimpleES\EventSourcing\Event\Stream\EventStream;
use SimpleES\EventSourcing\Identifier\Identifies;
use SimpleES\EventSourcing\Metadata\Metadata;
use SimpleES\EventSourcing\Timestamp\Timestamp;

/**
 * @copyright Copyright (c) 2015 Future500 B.V.
 * @author    Jasper N. Brouwer <jasper@future500.nl>
 */
final class DBALEventStoreTest extends MockeryTestCase
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
     * @var \Mockery\MockInterface
     */
    private $connection;

    public function setUp()
    {
        $this->eventNameResolver = Mockery::mock('SimpleES\EventSourcing\Event\NameResolver\ResolvesEventNames');
        $this->serializer        = Mockery::mock('SimpleES\EventSourcing\Serializer\SerializesData');
        $this->connection        = Mockery::mock('Doctrine\DBAL\Driver\Connection');

        $this->eventStore = new DBALEventStore(
            $this->eventNameResolver,
            $this->serializer,
            $this->connection,
            'event_store'
        );
    }

    public function tearDown()
    {
        $this->eventStore        = null;
        $this->eventNameResolver = null;
        $this->serializer        = null;
        $this->connection        = null;
    }

    /**
     * @test
     * @expectedException \SimpleES\DoctrineDBALBridge\Exception\TableNameIsUnsafe
     */
    public function itEnsuresTheTableNameIsSafe()
    {
        new DBALEventStore(
            $this->eventNameResolver,
            $this->serializer,
            $this->connection,
            'dash-is-unsafe'
        );
    }

    /**
     * @test
     */
    public function itCommitsAnEventStream()
    {
        $aggregateId = AggregateId::fromString('some-id');

        $this->serializer
            ->shouldReceive('serialize')
            ->times(3)
            ->with(Mockery::type('SimpleES\EventSourcing\Event\DomainEvent'))
            ->andReturn('{"foo": "bar"}');

        $this->serializer
            ->shouldReceive('serialize')
            ->times(3)
            ->with(Mockery::type('SimpleES\EventSourcing\Metadata\Metadata'))
            ->andReturn('{"bar": "foo"}');

        $sql = <<< EOQ
INSERT INTO event_store
(event_id, event_name, event_payload, aggregate_id, aggregate_version, took_place_at, metadata)
VALUES
(:event_id, :event_name, :event_payload, :aggregate_id, :aggregate_version, :took_place_at, :metadata)
EOQ;

        $stmt = Mockery::mock('Doctrine\DBAL\Driver\Statement');

        $stmt->shouldReceive('bindValue')->once()->with('event_id', 'event-1', Type::GUID);
        $stmt->shouldReceive('bindValue')->once()->with('event_name', 'an_event_happened', Type::STRING);
        $stmt->shouldReceive('bindValue')->once()->with('aggregate_version', 0, Type::INTEGER);
        $stmt->shouldReceive('bindValue')->once()->with('event_id', 'event-2', Type::GUID);
        $stmt->shouldReceive('bindValue')->once()->with('event_name', 'another_event_happened', Type::STRING);
        $stmt->shouldReceive('bindValue')->once()->with('aggregate_version', 1, Type::INTEGER);
        $stmt->shouldReceive('bindValue')->once()->with('event_id', 'event-3', Type::GUID);
        $stmt->shouldReceive('bindValue')->once()->with('event_name', 'yet_another_event_happened', Type::STRING);
        $stmt->shouldReceive('bindValue')->once()->with('aggregate_version', 2, Type::INTEGER);
        $stmt->shouldReceive('bindValue')->times(3)->with('event_payload', '{"foo": "bar"}', Type::TEXT);
        $stmt->shouldReceive('bindValue')->times(3)->with('aggregate_id', (string) $aggregateId, Type::GUID);
        $stmt->shouldReceive('bindValue')->times(3)->with('took_place_at', Mockery::type('string'), Type::STRING);
        $stmt->shouldReceive('bindValue')->times(3)->with('metadata', '{"bar": "foo"}', Type::TEXT);

        $stmt->shouldReceive('execute')->times(3);

        $this->connection
            ->shouldReceive('prepare')
            ->once()
            ->with($sql)
            ->andReturn($stmt);

        $eventStream = $this->createEventStream($aggregateId);

        $this->eventStore->commit($eventStream);

        $stmt->shouldHaveReceived('execute');
    }

    /**
     * @test
     */
    public function itReadsAnEventStream()
    {
        $aggregateId = AggregateId::fromString('some-id');

        $sql = <<< EOQ
SELECT event_id, event_name, event_payload, aggregate_version, took_place_at, metadata
FROM event_store
WHERE aggregate_id = :aggregate_id
ORDER BY aggregate_version ASC
EOQ;

        list($row1, $row2, $row3) = $this->createRows();

        $stmt = Mockery::mock('Doctrine\DBAL\Driver\Statement');

        $stmt->shouldReceive('bindValue')->once()->with('aggregate_id', (string) $aggregateId, Type::GUID);
        $stmt->shouldReceive('execute')->once();
        $stmt->shouldReceive('fetch')->times(4)->with(PDO::FETCH_ASSOC)->andReturn($row1, $row2, $row3, false);

        $this->connection
            ->shouldReceive('prepare')
            ->once()
            ->with($sql)
            ->andReturn($stmt);

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->once()
            ->with('an_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnEventHappened');

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->once()
            ->with('another_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnotherEventHappened');

        $this->eventNameResolver
            ->shouldReceive('resolveEventClass')
            ->once()
            ->with('yet_another_event_happened')
            ->andReturn('SimpleES\DoctrineDBALBridge\Test\Auxiliary\YetAnotherEventHappened');

        $this->serializer
            ->shouldReceive('deserialize')
            ->times(3)
            ->with('{"foo": "bar"}', Mockery::type('string'))
            ->andReturn(Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'));

        $this->serializer
            ->shouldReceive('deserialize')
            ->times(3)
            ->with('{"bar": "foo"}', 'SimpleES\EventSourcing\Metadata\Metadata')
            ->andReturn(new Metadata([]));

        $eventStream = $this->eventStore->read($aggregateId);

        $this->assertInstanceOf('SimpleES\EventSourcing\Event\Stream\EventStream', $eventStream);
        $this->assertCount(3, $eventStream);
    }

    /**
     * @test
     * @expectedException \SimpleES\EventSourcing\Exception\AggregateIdNotFound
     */
    public function itFailsToReadAnEventStreamWhenTheAggregateIdCannotBeFound()
    {
        $aggregateId = AggregateId::fromString('some-id');

        $sql = <<< EOQ
SELECT event_id, event_name, event_payload, aggregate_version, took_place_at, metadata
FROM event_store
WHERE aggregate_id = :aggregate_id
ORDER BY aggregate_version ASC
EOQ;

        $stmt = Mockery::mock('Doctrine\DBAL\Driver\Statement');

        $stmt->shouldReceive('bindValue')->once()->with('aggregate_id', (string) $aggregateId, Type::GUID);
        $stmt->shouldReceive('execute')->once();
        $stmt->shouldReceive('fetch')->once()->with(PDO::FETCH_ASSOC)->andReturn(false);

        $this->connection
            ->shouldReceive('prepare')
            ->once()
            ->with($sql)
            ->andReturn($stmt);

        $this->eventStore->read($aggregateId);
    }

    /**
     * @param Identifies $aggregateId
     *
     * @return EventStream
     */
    private function createEventStream(Identifies $aggregateId)
    {
        $envelope1 = Mockery::mock('SimpleES\EventSourcing\Event\Stream\EnvelopsEvent');
        $envelope1->shouldReceive('eventId')->andReturn(EventId::fromString('event-1'));
        $envelope1->shouldReceive('eventName')->andReturn('an_event_happened');
        $envelope1->shouldReceive('event')->andReturn(Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'));
        $envelope1->shouldReceive('aggregateId')->andReturn($aggregateId);
        $envelope1->shouldReceive('aggregateVersion')->andReturn(0);
        $envelope1->shouldReceive('tookPlaceAt')->andReturn(Timestamp::now());
        $envelope1->shouldReceive('metadata')->andReturn(new Metadata([]));

        $envelope2 = Mockery::mock('SimpleES\EventSourcing\Event\Stream\EnvelopsEvent');
        $envelope2->shouldReceive('eventId')->andReturn(EventId::fromString('event-2'));
        $envelope2->shouldReceive('eventName')->andReturn('another_event_happened');
        $envelope2->shouldReceive('event')->andReturn(Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'));
        $envelope2->shouldReceive('aggregateId')->andReturn($aggregateId);
        $envelope2->shouldReceive('aggregateVersion')->andReturn(1);
        $envelope2->shouldReceive('tookPlaceAt')->andReturn(Timestamp::now());
        $envelope2->shouldReceive('metadata')->andReturn(new Metadata([]));

        $envelope3 = Mockery::mock('SimpleES\EventSourcing\Event\Stream\EnvelopsEvent');
        $envelope3->shouldReceive('eventId')->andReturn(EventId::fromString('event-3'));
        $envelope3->shouldReceive('eventName')->andReturn('yet_another_event_happened');
        $envelope3->shouldReceive('event')->andReturn(Mockery::mock('SimpleES\EventSourcing\Event\DomainEvent'));
        $envelope3->shouldReceive('aggregateId')->andReturn($aggregateId);
        $envelope3->shouldReceive('aggregateVersion')->andReturn(2);
        $envelope3->shouldReceive('tookPlaceAt')->andReturn(Timestamp::now());
        $envelope3->shouldReceive('metadata')->andReturn(new Metadata([]));

        $eventStream = new EventStream(
            $aggregateId,
            [$envelope1, $envelope2, $envelope3]
        );

        return $eventStream;
    }

    /**
     * @return array
     */
    private function createRows()
    {
        $row1 = [
            'event_id'          => 'event-1',
            'event_name'        => 'an_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '0',
            'took_place_at'     => '2015-03-14T10:57:36.785643+0000',
            'metadata'          => '{"bar": "foo"}'
        ];
        $row2 = [
            'event_id'          => 'event-2',
            'event_name'        => 'another_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '1',
            'took_place_at'     => '2015-03-14T10:57:37.242328+0000',
            'metadata'          => '{"bar": "foo"}'
        ];
        $row3 = [
            'event_id'          => 'event-3',
            'event_name'        => 'yet_another_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '2',
            'took_place_at'     => '2015-03-14T10:57:37.731385+0000',
            'metadata'          => '{"bar": "foo"}'
        ];

        return [$row1, $row2, $row3];
    }
}
