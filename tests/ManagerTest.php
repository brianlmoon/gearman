<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Manager;
use PHPUnit\Framework\TestCase;

class ManagerTest extends TestCase
{
    public function testVersionSendsCommandAndReturnsTrimmedResponse(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite($peer, "gearmand 1.1.0\n");

        $this->assertSame('gearmand 1.1.0', $manager->version());
        $this->assertSame("version\r\n", $this->readCommandLine($peer));
    }

    public function testShutdownSendsGracefulCommandAndPreventsFurtherCommands(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite($peer, "OK\n");

        $this->assertTrue($manager->shutdown(true));
        $this->assertSame("shutdown graceful\r\n", $this->readCommandLine($peer));

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('This server has been shut down');
        $manager->version();
    }

    public function testWorkersParsesResponseFromServer(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite(
            $peer,
            "0 127.0.0.1 worker-a : ability_one ability_two\n" .
            "1 127.0.0.2 worker-b :\n" .
            ".\n"
        );

        $workers = $manager->workers();
        $this->assertSame("workers\r\n", $this->readCommandLine($peer));
        $this->assertSame(
            array(
                array(
                    'fd' => '0',
                    'ip' => '127.0.0.1',
                    'id' => 'worker-a',
                    'abilities' => array('ability_one', 'ability_two')
                ),
                array(
                    'fd' => '1',
                    'ip' => '127.0.0.2',
                    'id' => 'worker-b',
                    'abilities' => array()
                )
            ),
            $workers
        );
    }

    public function testStatusReturnsFunctionStats(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite(
            $peer,
            "sum\t2\t1\t3\n" .
            "reverse\t0\t0\t1\n" .
            ".\n"
        );

        $status = $manager->status();

        $this->assertSame("status\r\n", $this->readCommandLine($peer));
        $this->assertSame(
            array(
                'sum' => array(
                    'in_queue' => '2',
                    'jobs_running' => '1',
                    'capable_workers' => '3'
                ),
                'reverse' => array(
                    'in_queue' => '0',
                    'jobs_running' => '0',
                    'capable_workers' => '1'
                )
            ),
            $status
        );
    }

    public function testStatusThrowsExceptionWhenServerReturnsError(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite($peer, "ERR 123 Something+bad\n");

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Something+bad [error code: 123]');
        $manager->status();
    }

    public function testSetMaxQueueSizeRejectsInvalidFunctionName(): void
    {
        $manager = new TestableManager();

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Invalid function name');
        $manager->setMaxQueueSize('bad name', 5);
    }

    public function testSetMaxQueueSizeSendsCommandAndConfirmsResponse(): void
    {
        list($manager, $peer) = $this->createManagerWithSocketPair();
        fwrite($peer, "OK\n");

        $this->assertTrue($manager->setMaxQueueSize('job_name', 10));
        $this->assertSame("maxqueue job_name 10\r\n", $this->readCommandLine($peer));
    }

    private function createManagerWithSocketPair(): array
    {
        if (!function_exists('stream_socket_pair')) {
            $this->markTestSkipped('stream_socket_pair is not available on this platform');
        }

        $sockets = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, 0);
        if ($sockets === false) {
            $this->markTestSkipped('Unable to create socket pair for Manager tests');
        }

        $manager = new TestableManager();
        $manager->setConnection($sockets[0]);
        stream_set_blocking($sockets[1], true);

        return array($manager, $sockets[1]);
    }

    private function readCommandLine($peer): string
    {
        $line = fgets($peer);
        return $line === false ? '' : $line;
    }
}

class TestableManager extends Manager
{
    public function setConnection($conn): void
    {
        $this->conn = $conn;
    }
}
