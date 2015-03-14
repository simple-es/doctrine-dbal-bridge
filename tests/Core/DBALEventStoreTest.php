<?php

/**
 * @license https://github.com/simple-es/doctrine-dbal-bridge/blob/master/LICENSE MIT
 */

namespace SimpleES\DoctrineDBALBridge\Test\Core;

use Doctrine\DBAL\Types\Type;
use PDO;
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
final class DBALEventStoreTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var DBALEventStore
     */
    private $eventStore;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    private $eventNameResolver;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    private $serializer;

    /**
     * @var \PHPUnit_Framework_MockObject_MockObject
     */
    private $connection;

    public function setUp()
    {
        $this->eventNameResolver = $this->getMock('SimpleES\EventSourcing\Event\Resolver\ResolvesEventNames');
        $this->serializer        = $this->getMock('SimpleES\EventSourcing\Serializer\SerializesData');
        $this->connection        = $this->getMock('Doctrine\DBAL\Driver\Connection');

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
            ->expects($this->exactly(6))
            ->method('serialize')
            ->will(
                $this->returnValueMap(
                    [
                        [$this->isInstanceOf('SimpleES\EventSourcing\Event\DomainEvent'), '{"foo": "bar"}'],
                        [$this->isInstanceOf('SimpleES\EventSourcing\Metadata\Metadata'), '{"bar": "foo"}']
                    ]
                )
            );

        $sql = <<< EOQ
INSERT INTO event_store
(event_id, event_name, event_payload, aggregate_id, aggregate_version, took_place_at, metadata)
VALUES
(:event_id, :event_name, :event_payload, :aggregate_id, :aggregate_version, :took_place_at, :metadata)
EOQ;

        $statement = $this->getMock('Doctrine\DBAL\Driver\Statement');

        $statement
            ->expects($this->exactly(21))
            ->method('bindValue')
            ->will(
                $this->returnValueMap(
                    [
                        ['event_id', 'event-1', Type::GUID],
                        ['event_name', 'an_event_happened', Type::STRING],
                        ['event_payload', '{"foo": "bar"}', Type::TEXT],
                        ['aggregate_id', (string) $aggregateId, Type::GUID],
                        ['aggregate_version', 0, Type::INTEGER],
                        ['took_place_at', $this->isType('string'), Type::STRING],
                        ['metadata', '{"bar": "foo"}', Type::TEXT],
                        ['event_id', 'event-2', Type::GUID],
                        ['event_name', 'another_event_happened', Type::STRING],
                        ['event_payload', '{"foo": "bar"}', Type::TEXT],
                        ['aggregate_id', (string) $aggregateId, Type::GUID],
                        ['aggregate_version', 1, Type::INTEGER],
                        ['took_place_at', $this->isType('string'), Type::STRING],
                        ['metadata', '{"bar": "foo"}', Type::TEXT],
                        ['event_id', 'event-3', Type::GUID],
                        ['event_name', 'yet_another_event_happened', Type::STRING],
                        ['event_payload', '{"foo": "bar"}', Type::TEXT],
                        ['aggregate_id', (string) $aggregateId, Type::GUID],
                        ['aggregate_version', 2, Type::INTEGER],
                        ['took_place_at', $this->isType('string'), Type::STRING],
                        ['metadata', '{"bar": "foo"}', Type::TEXT]
                    ]
                )
            );

        $statement
            ->expects($this->exactly(3))
            ->method('execute');

        $this->connection
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->will($this->returnValue($statement));

        $eventStream = $this->createEventStream($aggregateId);

        $this->eventStore->commit($eventStream);
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

        list($rowOne, $rowTwo, $rowThree) = $this->createRows();

        $statement = $this->getMock('Doctrine\DBAL\Driver\Statement');

        $statement
            ->expects($this->once())
            ->method('bindValue')
            ->with('aggregate_id', (string) $aggregateId, Type::GUID);

        $statement
            ->expects($this->exactly(4))
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->will($this->onConsecutiveCalls($rowOne, $rowTwo, $rowThree, false));

        $this->connection
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->will($this->returnValue($statement));

        $this->eventNameResolver
            ->expects($this->exactly(3))
            ->method('resolveEventClass')
            ->willReturnMap(
                [
                    ['an_event_happened', 'SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnEventHappened'],
                    ['another_event_happened', 'SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnotherEventHappened'],
                    ['yet_another_event_happened', 'SimpleES\DoctrineDBALBridge\Test\Auxiliary\YetAnotherEventHappened']
                ]
            );

        $eventOne   = $this->getMock('SimpleES\EventSourcing\Event\DomainEvent');
        $eventTwo   = $this->getMock('SimpleES\EventSourcing\Event\DomainEvent');
        $eventThree = $this->getMock('SimpleES\EventSourcing\Event\DomainEvent');

        $this->serializer
            ->expects($this->exactly(6))
            ->method('deserialize')
            ->willReturnMap(
                [
                    ['{"foo": "bar"}', 'SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnEventHappened', $eventOne],
                    ['{"foo": "bar"}', 'SimpleES\DoctrineDBALBridge\Test\Auxiliary\AnotherEventHappened', $eventTwo],
                    [
                        '{"foo": "bar"}',
                        'SimpleES\DoctrineDBALBridge\Test\Auxiliary\YetAnotherEventHappened',
                        $eventThree
                    ],
                    ['{"bar": "foo"}', 'SimpleES\EventSourcing\Metadata\Metadata', new Metadata([])]
                ]
            );

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

        $statement = $this->getMock('Doctrine\DBAL\Driver\Statement');

        $statement
            ->expects($this->once())
            ->method('bindValue')
            ->with('aggregate_id', (string) $aggregateId, Type::GUID);

        $statement
            ->expects($this->once())
            ->method('fetch')
            ->with(PDO::FETCH_ASSOC)
            ->will($this->returnValue(false));

        $this->connection
            ->expects($this->once())
            ->method('prepare')
            ->with($sql)
            ->will($this->returnValue($statement));

        $this->eventStore->read($aggregateId);
    }

    /**
     * @param Identifies $aggregateId
     * @return EventStream
     */
    private function createEventStream(Identifies $aggregateId)
    {
        $envelopeOne = new EventEnvelope(
            EventId::fromString('event-1'),
            'an_event_happened',
            $this->getMock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            0,
            Timestamp::now(),
            new Metadata([])
        );

        $envelopeTwo = new EventEnvelope(
            EventId::fromString('event-2'),
            'another_event_happened',
            $this->getMock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            1,
            Timestamp::now(),
            new Metadata([])
        );

        $envelopeThree = new EventEnvelope(
            EventId::fromString('event-3'),
            'yet_another_event_happened',
            $this->getMock('SimpleES\EventSourcing\Event\DomainEvent'),
            $aggregateId,
            2,
            Timestamp::now(),
            new Metadata([])
        );

        $eventStream = new EventStream(
            $aggregateId,
            [$envelopeOne, $envelopeTwo, $envelopeThree]
        );

        return $eventStream;
    }

    /**
     * @return array
     */
    private function createRows()
    {
        $rowOne   = [
            'event_id'          => 'event-1',
            'event_name'        => 'an_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '0',
            'took_place_at'     => '2015-03-14T10:57:36.785643+0000',
            'metadata'          => '{"bar": "foo"}'
        ];
        $rowTwo   = [
            'event_id'          => 'event-2',
            'event_name'        => 'another_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '1',
            'took_place_at'     => '2015-03-14T10:57:37.242328+0000',
            'metadata'          => '{"bar": "foo"}'
        ];
        $rowThree = [
            'event_id'          => 'event-3',
            'event_name'        => 'yet_another_event_happened',
            'event_payload'     => '{"foo": "bar"}',
            'aggregate_version' => '2',
            'took_place_at'     => '2015-03-14T10:57:37.731385+0000',
            'metadata'          => '{"bar": "foo"}'
        ];

        return [$rowOne, $rowTwo, $rowThree];
    }
}
