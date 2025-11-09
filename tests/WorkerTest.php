<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Connection;
use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Worker;
use PHPUnit\Framework\TestCase;

class WorkerTest extends TestCase
{
    public function testConstructorRejectsEmptyServerList(): void
    {
        $this->expectException(Exception::class);
        new Worker(array());
    }

    public function testAddAbilityAnnouncesToAllConnections(): void
    {
        $worker = new TestWorker();
        $connA = new WorkerTestConnection();
        $connB = new WorkerTestConnection();

        $worker->setConnectionsForTest(array(
            'server-a' => $connA,
            'server-b' => $connB,
        ));

        $worker->addAbility('ReverseNumbers');

        $this->assertArrayHasKey('ReverseNumbers', $worker->getAbilitiesForTest());
        $this->assertSame(array(), $worker->getInitParamsForTest('ReverseNumbers'));

        $this->assertSame('can_do', $connA->sentCommands[0]['command']);
        $this->assertSame(array('func' => 'ReverseNumbers'), $connA->sentCommands[0]['params']);

        $this->assertSame('can_do', $connB->sentCommands[0]['command']);
    }

    public function testAddAbilityWithTimeoutUsesCanDoTimeout(): void
    {
        $worker = new TestWorker();
        $conn = new WorkerTestConnection();

        $worker->addAbility('EmailUsers', 45, array('batch' => true), $conn);

        $this->assertSame('can_do_timeout', $conn->sentCommands[0]['command']);
        $this->assertSame(
            array('func' => 'EmailUsers', 'timeout' => 45),
            $conn->sentCommands[0]['params']
        );
        $this->assertSame(array('batch' => true), $worker->getInitParamsForTest('EmailUsers'));
        $this->assertSame(45, $worker->getAbilitiesForTest()['EmailUsers']);
    }

    public function testAddAbilitiesReplaysExistingAbilitiesToNewConnection(): void
    {
        $worker = new TestWorker();
        $existing = new WorkerTestConnection();
        $worker->setConnectionsForTest(array('existing' => $existing));

        $worker->addAbility('ResizeImages', 30, array('quality' => 80));

        $late = new WorkerTestConnection();
        $worker->callAddAbilitiesForTest($late);

        $this->assertSame('can_do_timeout', $late->sentCommands[0]['command']);
        $this->assertSame(
            array('func' => 'ResizeImages', 'timeout' => 30),
            $late->sentCommands[0]['params']
        );
    }

    public function testConnectionStatusSummarizesConnectionsAndStats(): void
    {
        $worker = new TestWorker();
        $conn = new WorkerTestConnection();
        $worker->setConnectionsForTest(array('server-a:4730' => $conn));
        $worker->setRetryConnectionsForTest(array('server-b:4730' => time() + 10));
        $worker->setStatsForTest(array('server-a:4730' => 5));

        $status = $worker->connection_status();

        $this->assertSame(1, $status['connected']);
        $this->assertSame(1, $status['disconnected']);
        $this->assertSame(array('server-a:4730' => true, 'server-b:4730' => false), $status['servers']);
        $this->assertSame(array('server-a:4730' => 5), $status['stats']);
    }

    public function testRetryTimeBacksOffUntilMax(): void
    {
        $worker = new TestWorker();

        $this->assertSame(3, $worker->callRetryTimeForTest(1));
        $this->assertSame(6, $worker->callRetryTimeForTest(2));
        $this->assertSame(12, $worker->callRetryTimeForTest(3));
        $this->assertSame(24, $worker->callRetryTimeForTest(4));
        $this->assertSame(48, $worker->callRetryTimeForTest(5));
        $this->assertSame(60, $worker->callRetryTimeForTest(6));
    }

    public function testCallbacksInvokeRegisteredHandlers(): void
    {
        $worker = new TestWorker();
        $conn = new WorkerTestConnection();
        $worker->setConnectionsForTest(array('srv' => $conn));
        $worker->setFailedConnectionsForTest(array('srv' => 2));

        $events = array();
        $worker->attachCallback(function ($handle, $job, $args) use (&$events) {
            $events['start'][] = array($handle, $job, $args);
        }, Worker::JOB_START);

        $worker->attachCallback(function ($handle, $job, $result) use (&$events) {
            $events['complete'][] = array($handle, $job, $result);
        }, Worker::JOB_COMPLETE);

        $worker->attachCallback(function ($handle, $job, $error) use (&$events) {
            $events['fail'][] = array($handle, $job, $error->getMessage());
        }, Worker::JOB_FAIL);

        $worker->attachCallback(function ($message, $server, $connected, $failed) use (&$events) {
            $events['status'][] = array($message, $server, $connected, $failed);
        }, Worker::WORKER_STATUS);

        $worker->invokeStartForTest('H:1', 'ResizeImages', array('file' => 'foo'));
        $worker->invokeCompleteForTest('H:1', 'ResizeImages', array('result' => true));
        $worker->invokeFailForTest('H:1', 'ResizeImages', new Exception('boom'));
        $worker->invokeStatusForTest('Connected', 'srv');

        $this->assertSame(array(array('H:1', 'ResizeImages', array('file' => 'foo'))), $events['start']);
        $this->assertSame(array(array('H:1', 'ResizeImages', array('result' => true))), $events['complete']);
        $this->assertSame(array(array('H:1', 'ResizeImages', 'boom')), $events['fail']);
        $this->assertSame(
            array(array('Connected', 'srv', true, 2)),
            $events['status']
        );
    }

    public function testEndWorkClosesConnectionsAndResetsAbilities(): void
    {
        $worker = new TestWorker();
        $conn = new WorkerTestConnection();
        $worker->setConnectionsForTest(array('srv' => $conn));

        $worker->endWork();

        $this->assertTrue($conn->closed);
        $this->assertSame('reset_abilities', $conn->sentCommands[0]['command']);
        $this->assertSame(array(), $worker->getConnectionsForTest());
    }

    public function testBeginWorkStopsWhenMonitorRequests(): void
    {
        $worker = new LoopTestWorker();
        $worker->setConnectionsForTest(array('srv' => new WorkerTestConnection()));
        $worker->askReturnQueue = array(true, false);
        $worker->retryReturnQueue = array(false, false);

        $iterations = 0;
        $worker->beginWork(function () use (&$iterations) {
            $iterations++;
            return $iterations >= 2;
        });

        $this->assertSame(2, $worker->askCalls);
        $this->assertSame(4, $worker->retryCalls);
        $this->assertTrue($worker->waitQuietlyCalled);
        $this->assertFalse($worker->deepSleepCalled);
    }
}

class TestWorker extends Worker
{
    public function __construct()
    {
        // Skip the real constructor to avoid socket connections.
    }

    public function setConnectionsForTest(array $connections): void
    {
        $this->conn = $connections;
    }

    public function getConnectionsForTest(): array
    {
        return $this->conn;
    }

    public function setRetryConnectionsForTest(array $retryConnections): void
    {
        $this->retryConn = $retryConnections;
    }

    public function setStatsForTest(array $stats): void
    {
        $this->stats = $stats;
    }

    public function setFailedConnectionsForTest(array $failed): void
    {
        $this->failedConn = $failed;
    }

    public function callAddAbilitiesForTest(?Connection $conn = null): void
    {
        $this->addAbilities($conn);
    }

    public function callRetryTimeForTest(int $count): int
    {
        return $this->retryTime($count);
    }

    public function getAbilitiesForTest(): array
    {
        return $this->abilities;
    }

    public function getInitParamsForTest(string $ability): array
    {
        return $this->initParams[$ability] ?? array();
    }

    public function invokeStartForTest(string $handle, string $job, $args): void
    {
        $this->start($handle, $job, $args);
    }

    public function invokeCompleteForTest(string $handle, string $job, array $result): void
    {
        $this->complete($handle, $job, $result);
    }

    public function invokeFailForTest(string $handle, string $job, Exception $error): void
    {
        $this->fail($handle, $job, $error);
    }

    public function invokeStatusForTest(string $message, ?string $server = null): void
    {
        $this->status($message, $server);
    }
}

class LoopTestWorker extends TestWorker
{
    /** @var array<int, bool> */
    public array $askReturnQueue = array();
    /** @var array<int, bool> */
    public array $retryReturnQueue = array();
    public int $askCalls = 0;
    public int $retryCalls = 0;
    public bool $waitQuietlyCalled = false;
    public bool $deepSleepCalled = false;

    protected function askForWork(?callable $monitor = null, ?int $lastJobTime = null): bool
    {
        $this->askCalls++;
        return array_shift($this->askReturnQueue) ?? false;
    }

    protected function retryConnections(): bool
    {
        $this->retryCalls++;
        return array_shift($this->retryReturnQueue) ?? false;
    }

    protected function waitQuietly(callable $monitor, int $lastJobTime): bool
    {
        $this->waitQuietlyCalled = true;
        return false;
    }

    protected function deepSleep(callable $monitor, int $lastJobTime): void
    {
        $this->deepSleepCalled = true;
    }
}

class WorkerTestConnection extends Connection
{
    /** @var array<int, array{command:string,params:array}> */
    public array $sentCommands = array();
    public bool $closed = false;
    public bool $connected = true;

    public function __construct()
    {
        // Avoid establishing a real socket.
    }

    public function send(string $command, array $params = array()): void
    {
        $this->sentCommands[] = array('command' => $command, 'params' => $params);
    }

    public function close(): void
    {
        $this->closed = true;
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }
}
