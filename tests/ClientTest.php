<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Client;
use Moonspot\Gearman\Connection;
use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Set;
use Moonspot\Gearman\Task;
use PHPUnit\Framework\TestCase;

class ClientTest extends TestCase
{
    public function testConstructorRejectsEmptyServerList(): void
    {
        $this->expectException(Exception::class);
        new Client(array());
    }

    public function testGetInstanceReturnsSameInstanceForSameConfiguration(): void
    {
        $servers = array('127.0.0.77:4730');
        $first = Client::getInstance($servers, 250);
        $second = Client::getInstance($servers, 250);

        $this->assertSame($first, $second);
    }

    public function testGetInstanceReturnsDifferentInstanceForDifferentConfigurations(): void
    {
        $first = Client::getInstance(array('127.0.0.88:4730'), 250);
        $second = Client::getInstance(array('127.0.0.89:4730'), 250);

        $this->assertNotSame($first, $second);
    }

    public function testMagicCallCreatesBackgroundTaskAndReturnsHandle(): void
    {
        $client = new RunSetCapturingClient('127.0.0.1:4730');
        $handle = $client->processReport(array('foo' => 'bar'));

        $this->assertSame('handle-from-runset', $handle);

        $task = $client->getLastTask();
        $this->assertInstanceOf(Task::class, $task);
        $this->assertSame(Task::JOB_BACKGROUND, $task->type);
        $this->assertSame(array('foo' => 'bar'), $task->arg);
    }

    public function testSubmitTaskEncodesArgumentsAndAddsWaitingTask(): void
    {
        $connection = new RecordingConnection();
        $client = new SubmitTaskClient('127.0.0.1:4730');
        $client->connection = $connection;

        $task = new Task(
            'reverse',
            array('payload' => 1),
            'uniq-low-bg',
            Task::JOB_LOW_BACKGROUND,
            array('10.0.0.1:4730')
        );

        $client->submitTaskProxy($task);

        $this->assertCount(1, $connection->sentCommands);
        $this->assertSame('submit_job_low_bg', $connection->sentCommands[0]['command']);
        $this->assertSame(json_encode(array('payload' => 1)), $connection->sentCommands[0]['params']['arg']);
        $this->assertSame($task, $connection->waitingTasks[0]);

        $this->assertSame('uniq-low-bg', $client->connectionCalls[0]['uniq']);
        $this->assertSame(array('10.0.0.1:4730'), $client->connectionCalls[0]['servers']);
    }

    public function testHandleResponseProcessesWorkComplete(): void
    {
        $client = new SubmitTaskClient('127.0.0.1:4730');

        $task = new Task('sum', array(1, 2), 'uniq-handle');
        $set = new Set(array($task));
        $set->handles['H:1'] = $task->uniq;

        $response = array(
            'function' => 'work_complete',
            'data' => array(
                'handle' => 'H:1',
                'result' => json_encode(array('ok' => true))
            )
        );

        $client->handleResponseProxy($response, new RecordingConnection(), $set);

        $this->assertSame(0, $set->tasksCount);
        $this->assertTrue($task->finished);
        $this->assertSame(array('ok' => true), $task->result);
    }

    public function testHandleResponseProcessesJobCreatedForBackgroundTask(): void
    {
        $client = new SubmitTaskClient('127.0.0.1:4730');
        $task = new Task('sum', array(), 'uniq-bg', Task::JOB_BACKGROUND);
        $set = new Set(array($task));

        $connection = new RecordingConnection();
        $connection->waitingTasks[] = $task;

        $response = array(
            'function' => 'job_created',
            'data' => array('handle' => 'H:2')
        );

        $client->handleResponseProxy($response, $connection, $set);

        $this->assertSame('H:2', $task->handle);
        $this->assertTrue($task->finished);
        $this->assertSame($task->uniq, $set->handles['H:2']);
    }

    public function testDisconnectClosesTrackedConnections(): void
    {
        $client = new SubmitTaskClient('127.0.0.1:4730');
        $connection = new RecordingConnection();

        $client->setConnectionsForTest(array('127.0.0.1:4730' => $connection));

        $client->disconnect();

        $this->assertTrue($connection->closed);
        $this->assertSame(array(), $client->getConnectionsForTest());
    }
}

class RunSetCapturingClient extends Client
{
    private ?Set $lastRunSet = null;

    public function runSet(Set $set, ?int $timeout = null): void
    {
        $this->lastRunSet = $set;
        foreach ($set->tasks as $task) {
            $task->handle = 'handle-from-runset';
            break;
        }
    }

    public function getLastTask(): ?Task
    {
        if ($this->lastRunSet === null) {
            return null;
        }

        $key = array_key_first($this->lastRunSet->tasks);
        return $key === null ? null : $this->lastRunSet->tasks[$key];
    }
}

class SubmitTaskClient extends Client
{
    public ?RecordingConnection $connection = null;
    /** @var array<int, array{uniq:?string,servers:?array}> */
    public array $connectionCalls = array();

    protected function getConnection(?string $uniq = null, ?array $servers = null): Connection
    {
        $this->connectionCalls[] = array('uniq' => $uniq, 'servers' => $servers);
        if ($this->connection instanceof RecordingConnection) {
            return $this->connection;
        }

        return parent::getConnection($uniq, $servers);
    }

    public function submitTaskProxy(Task $task): void
    {
        $this->submitTask($task);
    }

    public function handleResponseProxy(array $resp, Connection $conn, Set $tasks): void
    {
        $this->handleResponse($resp, $conn, $tasks);
    }

    public function setConnectionsForTest(array $connections): void
    {
        $this->conn = $connections;
    }

    public function getConnectionsForTest(): array
    {
        return $this->conn;
    }
}

class RecordingConnection extends Connection
{
    /** @var array<int, array{command:string,params:array}> */
    public array $sentCommands = array();
    /** @var array<int, Task> */
    public array $waitingTasks = array();
    public bool $connected = true;
    public bool $closed = false;

    public function __construct()
    {
        // Skip the parent constructor so no socket connection is attempted.
    }

    public function isConnected(): bool
    {
        return $this->connected;
    }

    public function send(string $command, array $params = array()): void
    {
        $this->sentCommands[] = array('command' => $command, 'params' => $params);
    }

    public function addWaitingTask(Task $task): void
    {
        $this->waitingTasks[] = $task;
    }

    public function getWaitingTask(): ?Task
    {
        return array_shift($this->waitingTasks);
    }

    public function close(): void
    {
        $this->closed = true;
    }
}
