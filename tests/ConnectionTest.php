<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Connection;
use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Task;
use PHPUnit\Framework\TestCase;

class ConnectionTest extends TestCase
{
    protected function tearDown(): void
    {
        TestConnection::resetMultiByteSupport();
    }

    public function testAddWaitingTaskFollowsFifoOrder(): void
    {
        $connection = new TestConnection();
        $first = new Task('first', array('a' => 1));
        $second = new Task('second', array('b' => 2));

        $connection->addWaitingTask($first);
        $connection->addWaitingTask($second);

        $this->assertSame($first, $connection->getWaitingTask());
        $this->assertSame($second, $connection->getWaitingTask());
        $this->assertNull($connection->getWaitingTask());
    }

    public function testSendWritesBinaryCommandToSocket(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->initializeMagicMap();
            $connection->setSocket($local);

            $connection->send('can_do', array('func' => 'resize'));

            $written = socket_read($remote, 4096);
            $this->assertNotFalse($written);
            $this->assertSame("\0REQ", substr($written, 0, 4));

            $header = unpack('Ntype/Nlen', substr($written, 4, 8));
            $this->assertSame(1, $header['type']);
            $this->assertSame(6, $header['len']);

            $payload = substr($written, 12);
            $this->assertSame('resize', $payload);
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testReadParsesJobCreatedResponse(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->initializeMagicMap();
            $connection->setSocket($local);

            $payload = "H:1";
            $message = "\0RES" . pack('NN', 8, strlen($payload)) . $payload;
            socket_write($remote, $message);

            $response = $connection->read();

            $this->assertSame('job_created', $response['function']);
            $this->assertSame('H:1', $response['data']['handle']);
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testReadThrowsOnErrorResponse(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->initializeMagicMap();
            $connection->setSocket($local);

            $payload = implode("\x00", array('5', 'Whoops'));
            $message = "\0RES" . pack('NN', 19, strlen($payload)) . $payload;
            socket_write($remote, $message);

            $this->expectException(Exception::class);
            $this->expectExceptionMessage('(5): Whoops');
            $connection->read();
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testBlockingReadReturnsResponseWhenDataArrives(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->initializeMagicMap();
            $connection->setSocket($local);

            $payload = "H:99";
            $message = "\0RES" . pack('NN', 8, strlen($payload)) . $payload;
            socket_write($remote, $message);

            $response = $connection->blockingRead(50);
            $this->assertSame('job_created', $response['function']);
            $this->assertSame('H:99', $response['data']['handle']);
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testBlockingReadThrowsOnTimeout(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->initializeMagicMap();
            $connection->setSocket($local);

            $this->expectException(Exception::class);
            $this->expectExceptionMessageMatches('/Socket timeout/');
            $connection->blockingRead(10);
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testIsConnectedDetectsSocketResource(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->setSocket($local);

            $this->assertTrue($connection->isConnected());

            $connection->setSocket(null);
            $this->assertFalse($connection->isConnected());
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    public function testFixTimeoutConvertsSecondsToMillisecondsForModernServers(): void
    {
        $connection = new TestConnection();
        $connection->initializeMagicMap();
        $connection->setServerVersionForTest('1.1.20');

        $result = $connection->fixTimeoutProxy(array('timeout' => 3));
        $this->assertSame(3000, $result['timeout']);
    }

    public function testFixTimeoutLeavesSecondsForLegacyServers(): void
    {
        $connection = new TestConnection();
        $connection->initializeMagicMap();
        $connection->setServerVersionForTest('1.1.18');

        $result = $connection->fixTimeoutProxy(array('timeout' => 3));
        $this->assertSame(3, $result['timeout']);
    }

    public function testCalculateTimeoutHandlesSubSecondAndMultiSecondValues(): void
    {
        $this->assertSame(array(0, 500000), Connection::calculateTimeout(500));
        $this->assertSame(array(2.0, 250000.0), Connection::calculateTimeout(2250));
    }

    public function testSendThrowsOnUnknownCommand(): void
    {
        [$local, $remote] = $this->createSocketPair();
        try {
            $connection = new TestConnection();
            $connection->setSocket($local);

            $this->expectException(Exception::class);
            $this->expectExceptionMessage('Invalid command');
            $connection->send('does_not_exist', array());
        } finally {
            $this->closeSockets($local, $remote);
        }
    }

    private function createSocketPair(): array
    {
        if (!function_exists('socket_create_pair')) {
            $this->markTestSkipped('Sockets extension is required');
        }

        $pair = array();
        if (!socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair)) {
            $this->fail('Unable to create socket pair: ' . socket_strerror(socket_last_error()));
        }

        return $pair;
    }

    private function closeSockets($first, $second): void
    {
        foreach (array($first, $second) as $socket) {
            if ($socket instanceof \Socket || (is_resource($socket) && get_resource_type($socket) === 'Socket')) {
                socket_close($socket);
            }
        }
    }
}

class TestConnection extends Connection
{
    public function __construct()
    {
        // Skip parent constructor to avoid network calls.
    }

    public function setSocket($socket): void
    {
        $this->socket = $socket;
    }

    public function initializeMagicMap(): void
    {
        if (!count($this->magic)) {
            foreach ($this->commands as $cmd => $definition) {
                $this->magic[$definition[0]] = array($cmd, $definition[1]);
            }
        }
    }

    public function setServerVersionForTest(?string $version): void
    {
        $this->serverVersion = $version;
    }

    public function fixTimeoutProxy(array $params): array
    {
        return $this->fixTimeout($params);
    }

    public static function resetMultiByteSupport(): void
    {
        self::$multiByteSupport = null;
    }
}
