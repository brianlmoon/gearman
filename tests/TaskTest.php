<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Client;
use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Set;
use Moonspot\Gearman\Task;
use PHPUnit\Framework\TestCase;

/**
 * @internal
 *
 * @coversNothing
 */
class TaskTest extends TestCase {
    protected array $task_callback = [];

    /**
     * Unknown job type.
     */
    public function testExceptionFromConstruct() {
        $this->expectException(Exception::class);
        new Task('foo', [], null, 8);
    }

    /**
     * Test parameters.
     */
    public function testParameters() {
        $uniq = uniqid();
        $task = new Task('foo', ['bar'], $uniq, 1);

        $this->assertEquals('foo', $task->func);
        $this->assertEquals(['bar'], $task->arg);
        $this->assertEquals($uniq, $task->uniq);
    }

    public function testAttachInvalidCallbackType() {
        $this->expectException(Exception::class);
        $task = new Task('foo', []);
        $this->assertInstanceOf('Task', $task->attachCallback('strlen', 666));
    }

    public function callbackProvider() {
        return [
            ['strlen', Task::TASK_FAIL],
            ['intval', Task::TASK_COMPLETE],
            ['explode', Task::TASK_STATUS],
        ];
    }

    /**
     * @dataProvider callbackProvider
     *
     * @param mixed $func
     * @param mixed $type
     */
    public function testAttachCallback($func, $type) {
        $task = new Task('foo', []);
        $task->attachCallback($func, $type);

        $callbacks = $task->getCallbacks();

        $this->assertEquals($func, $callbacks[$type][0]);
    }

    /**
     * Run the complete callback.
     */
    public function testCompleteCallback() {
        $task = new Task('foo', ['foo' => 'bar']);

        $this->assertEquals(null, $task->complete('foo'));

        // Attach a callback for real
        $task->attachCallback([$this, 'taskCallBack']);

        // build result and call complete again
        $json = json_decode('{"foo":"bar"}');
        $task->complete($json);

        $this->assertEquals($json, $task->result);

        $this->assertEquals(
            ['func' => 'foo', 'handle' => '', 'result' => $json],
            $this->task_callback
        );

        $this->task_callback = [];
    }

    /**
     * See that task has handle and server assigned.
     *
     * @group functional
     */
    public function testTaskStatus() {
        $client = new Client([GEARMAN_TEST_SERVER]);

        $task = new Task('Reverse', range(1, 5));
        $task->type = Task::JOB_BACKGROUND;

        $set = new Set();
        $set->addTask($task);

        $client->runSet($set);

        $this->assertNotEquals('', $task->handle);
    }

    /**
     * A test callback.
     *
     * @param string $func
     * @param string $handle
     * @param mixed  $result
     */
    public function taskCallBack($func, $handle, $result) {
        $this->task_callback = [
            'func'   => $func,
            'handle' => $handle,
            'result' => $result,
        ];
    }
}
