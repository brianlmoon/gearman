<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Task;
use PHPUnit\Framework\TestCase;

class TaskTest extends TestCase
{
    public function testConstructorGeneratesUniqueWhenNotProvided(): void
    {
        $func = 'reverse';
        $arg = array('payload' => 'abc');

        $task = new Task($func, $arg);

        $this->assertSame(md5($func . serialize($arg) . Task::JOB_NORMAL), $task->uniq);
        $this->assertSame(Task::JOB_NORMAL, $task->type);
        $this->assertSame(array(), $task->servers);
    }

    public function testConstructorHonorsProvidedUniqAndServers(): void
    {
        $servers = array('10.0.0.1:4730');
        $task = new Task('sum', array(1, 2), 'custom-uniq', Task::JOB_HIGH, $servers);

        $this->assertSame('custom-uniq', $task->uniq);
        $this->assertSame($servers, $task->servers);
        $this->assertSame(Task::JOB_HIGH, $task->type);
    }

    public function testConstructorRejectsUnknownType(): void
    {
        $this->expectException(Exception::class);
        new Task('sum', array(), 'uniq', 999);
    }

    public function testAttachCallbackValidatesCallable(): void
    {
        $task = new Task('sum', array());

        $this->expectException(Exception::class);
        $task->attachCallback('not_a_real_function');
    }

    public function testAttachCallbackValidatesType(): void
    {
        $task = new Task('sum', array());

        $this->expectException(Exception::class);
        $task->attachCallback(static function (): void {
        }, 999);
    }

    public function testCompleteInvokesCallbacksAndStoresResult(): void
    {
        $task = new Task('sum', array());
        $task->handle = 'H:1';
        $task->uniq = 'uniq-123';
        $captured = array();

        $returned = $task->attachCallback(static function (string $func, string $handle, $result, string $uniq) use (&$captured): void {
            $captured[] = compact('func', 'handle', 'result', 'uniq');
        });

        $this->assertSame($task, $returned);

        $result = array('ok' => true);
        $task->complete($result);

        $this->assertTrue($task->finished);
        $this->assertSame($result, $task->result);
        $this->assertCount(1, $captured);
        $this->assertSame(
            array(
                'func' => 'sum',
                'handle' => 'H:1',
                'result' => $result,
                'uniq' => 'uniq-123'
            ),
            $captured[0]
        );
    }

    public function testFailInvokesCallbacksWithTask(): void
    {
        $task = new Task('sum', array());
        $received = null;

        $task->attachCallback(static function (Task $failedTask) use (&$received): void {
            $received = $failedTask;
        }, Task::TASK_FAIL);

        $task->fail();

        $this->assertTrue($task->finished);
        $this->assertSame($task, $received);
    }

    public function testStatusInvokesCallbacksWithProgress(): void
    {
        $task = new Task('sum', array());
        $task->handle = 'H:2';
        $calls = array();

        $task->attachCallback(static function (string $func, string $handle, int $numerator, int $denominator) use (&$calls): void {
            $calls[] = compact('func', 'handle', 'numerator', 'denominator');
        }, Task::TASK_STATUS);

        $task->status(3, 7);

        $this->assertSame(
            array(
                array(
                    'func' => 'sum',
                    'handle' => 'H:2',
                    'numerator' => 3,
                    'denominator' => 7
                )
            ),
            $calls
        );
    }
}
