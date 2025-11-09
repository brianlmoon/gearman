<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Exception;
use Moonspot\Gearman\Set;
use Moonspot\Gearman\Task;
use PHPUnit\Framework\TestCase;

class SetTest extends TestCase
{
    public function testConstructorAddsUniqueTasksAndCountsThem(): void
    {
        $taskA = new Task('jobA', array('a'), 'uniq-a');
        $taskB = new Task('jobB', array('b'), 'uniq-b');

        $set = new Set(array($taskA, $taskB));

        $this->assertSame(2, $set->tasksCount);
        $this->assertSame(
            array(
                'uniq-a' => $taskA,
                'uniq-b' => $taskB
            ),
            $set->tasks
        );
    }

    public function testAddTaskIgnoresDuplicatesBasedOnUniq(): void
    {
        $task = new Task('job', array(), 'uniq-dup');
        $set = new Set();

        $set->addTask($task);
        $set->addTask($task);

        $this->assertSame(1, $set->tasksCount);
        $this->assertSame(array('uniq-dup' => $task), $set->tasks);
    }

    public function testGetTaskReturnsTaskMatchingHandle(): void
    {
        $task = new Task('job', array(), 'uniq-handle');
        $set = new Set(array($task));
        $set->handles['H:1'] = 'uniq-handle';

        $this->assertSame($task, $set->getTask('H:1'));
    }

    public function testGetTaskThrowsWhenHandleUnknown(): void
    {
        $set = new Set();

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Unknown handle');
        $set->getTask('missing');
    }

    public function testGetTaskThrowsWhenTaskMissingForHandle(): void
    {
        $set = new Set();
        $set->handles['H:1'] = 'ghost-task';

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('No task by that handle');
        $set->getTask('H:1');
    }

    public function testFinishedRunsCallbackWithTaskResultsWhenComplete(): void
    {
        $taskA = new Task('jobA', array(), 'uniq-a');
        $taskB = new Task('jobB', array(), 'uniq-b');
        $set = new Set(array($taskA, $taskB));

        $taskA->result = array('value' => 1);
        $taskB->result = array('value' => 2);
        $set->tasksCount = 0;

        $captured = null;
        $invocations = 0;
        $set->attachCallback(static function (array $results) use (&$captured, &$invocations): void {
            $invocations++;
            $captured = $results;
        });

        $this->assertTrue($set->finished());
        $this->assertSame(1, $invocations);
        $this->assertSame(array(array('value' => 1), array('value' => 2)), $captured);
    }

    public function testFinishedReturnsFalseWhenTasksRemain(): void
    {
        $set = new Set(array(new Task('job', array(), 'uniq')));

        $this->assertFalse($set->finished());
    }

    public function testIteratorAndCountExposeTasksCollection(): void
    {
        $task = new Task('job', array(), 'uniq');
        $set = new Set(array($task));

        $this->assertSame(array('uniq' => $task), iterator_to_array($set->getIterator()));
        $this->assertCount(1, $set);
    }
}
