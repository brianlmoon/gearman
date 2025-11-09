<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Client;
use Moonspot\Gearman\Set;
use Moonspot\Gearman\Task;
use Moonspot\Gearman\Worker;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;
use RuntimeException;

class ClientWorkerFunctionalTest extends TestCase
{
    #[Group('functional')]
    public function testClientReceivesResponseFromLocalWorker(): void
    {
        if (!extension_loaded('pcntl')) {
            $this->markTestSkipped('pcntl extension is required to run this functional test.');
        }

        if (!defined('NET_GEARMAN_TEST_SERVER')) {
            $this->markTestSkipped('NET_GEARMAN_TEST_SERVER is not defined.');
        }

        $server = NET_GEARMAN_TEST_SERVER;
        $this->assertGearmanServerReachable($server);

        $jobPath = __DIR__ . '/../Fixtures/Jobs/FunctionalEchoJob.php';
        $this->assertFileExists($jobPath, 'Fixture job file missing.');

        $workerPid = $this->forkWorker($server, $jobPath);

        try {
            $payload = array(
                'text' => 'ping',
                'uniq' => uniqid('functional_', true),
            );

            $task = new Task('FunctionalEchoJob', $payload);
            $result = null;

            $task->attachCallback(function ($func, $handle, $response) use (&$result): void {
                $result = $response;
            });

            $set = new Set(array($task));
            $client = new Client($server);
            $client->runSet($set, 5);

            $this->assertNotNull($result, 'Client did not receive a result from worker.');
            $this->assertSame('FunctionalEchoJob', $result['job']);
            $this->assertSame($payload, $result['payload']);
            $this->assertArrayHasKey('worker_pid', $result);
            $this->assertIsInt($result['worker_pid']);
        } finally {
            $this->terminateWorker($workerPid);
        }
    }

    private function forkWorker(string $server, string $jobPath): int
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            throw new RuntimeException('Failed to fork worker process.');
        }

        if ($pid === 0) {
            $this->runWorkerProcess($server, $jobPath);
            exit(0);
        }

        // Small delay to give the worker time to connect before client submits.
        usleep(300000);

        return $pid;
    }

    private function runWorkerProcess(string $server, string $jobPath): void
    {
        pcntl_async_signals(true);

        $worker = new Worker($server);
        $worker->addAbility(
            'FunctionalEchoJob',
            null,
            array(
                'path' => $jobPath,
                'class_name' => 'Moonspot\\Gearman\\Job\\FunctionalEchoJob',
            )
        );

        $state = new class {
            public int $completed = 0;
        };

        $worker->attachCallback(function () use ($state): void {
            $state->completed++;
        }, Worker::JOB_COMPLETE);

        $timeoutAt = microtime(true) + 15;

        pcntl_signal(SIGTERM, function () use ($worker): void {
            $worker->endWork();
            exit(0);
        });

        $worker->beginWork(function () use ($state, $timeoutAt): bool {
            if ($state->completed > 0) {
                return true;
            }

            return microtime(true) >= $timeoutAt;
        });

        $worker->endWork();
    }

    private function terminateWorker(int $pid): void
    {
        $status = null;
        $start = microtime(true);

        do {
            $result = pcntl_waitpid($pid, $status, WNOHANG);
            if ($result === $pid) {
                return;
            }
            usleep(100000);
        } while (microtime(true) - $start < 2);

        if (function_exists('posix_kill')) {
            posix_kill($pid, SIGTERM);
        }

        pcntl_waitpid($pid, $status);
    }

    private function assertGearmanServerReachable(string $server): void
    {
        $parts = explode(':', $server, 2);
        $host = $parts[0];
        $port = isset($parts[1]) ? (int)$parts[1] : 4730;

        $socket = @fsockopen($host, $port, $errno, $errstr, 1.0);

        if ($socket === false) {
            $this->markTestSkipped(
                sprintf(
                    'Gearman server not reachable at %s:%d (%s).',
                    $host,
                    $port,
                    $errstr ?: $errno
                )
            );
        }

        fclose($socket);
    }
}
