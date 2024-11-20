<?php

namespace Moonspot\Gearman;

/**
 * Interface for Danga's Gearman job scheduling system.
 *
 * PHP version 8.1+
 *
 * LICENSE: This source file is subject to the New BSD license that is
 * available through the world-wide-web at the following URI:
 * http://www.opensource.org/licenses/bsd-license.php. If you did not receive
 * a copy of the New BSD License and are unable to obtain it through the web,
 * please send a note to license@php.net so we can mail you a copy immediately.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @version   CVS: $Id$
 *
 * @see      https://github.com/brianlmoon/net_gearman
 */

/**
 * A class for creating sets of tasks.
 *
 * <code>
 * <?php
 * require_once 'Net/Gearman/Client.php';
 *
 * // This is the callback function for our tasks
 * function echoResult($result) {
 *     echo 'The result was: ' . $result . "\n";
 * }
 *
 * // Job name is the key, arguments to job are in the value array
 * $jobs = array(
 *     'AddTwoNumbers' => array('1', '2'),
 *     'Multiply' => array('3', '4')
 * );
 *
 * $set = new \Moonspot\Gearman\Set();
 * foreach ($jobs as $job => $args) {
 *     $task = new \Moonspot\Gearman\Task($job, $args);
 *     $task->attachCallback('echoResult');
 *     $set->addTask($task);
 * }
 *
 * $client = new \Moonspot\Gearman\Client(array(
 *     '127.0.0.1:7003', '127.0.0.1:7004'
 * ));
 *
 * $client->runSet($set);
 *
 * ?>
 * </code>
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @see      https://github.com/brianlmoon/net_gearman
 * @see       Moonspot\Gearman\Job_Common, Moonspot\Gearman\Worker
 */
class Set implements \IteratorAggregate, \Countable {
    /**
     * Tasks count.
     */
    public int $tasksCount = 0;

    /**
     * Tasks to run.
     */
    public array $tasks = [];

    /**
     * Handle to task mapping.
     */
    public array $handles = [];

    /**
     * Callback registered for set.
     */
    protected mixed $callback;

    /**
     * Constructor.
     *
     * @param array $tasks Array of tasks to run
     *
     * @see Moonspot\Gearman\Task
     */
    public function __construct(array $tasks = []) {
        foreach ($tasks as $task) {
            $this->addTask($task);
        }
    }

    /**
     * Add a task to the set.
     *
     * @param Task $task Task to add to the set
     *
     * @see Moonspot\Gearman\Task, Moonspot\Gearman\Set::$tasks
     */
    public function addTask(Task $task): void {
        if (!isset($this->tasks[$task->uniq])) {
            $this->tasks[$task->uniq] = $task;
            ++$this->tasksCount;
        }
    }

    /**
     * Get a task.
     *
     * @param string $handle Handle of task to get
     *
     * @return Task Instance of task
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function getTask(string $handle): Task {
        if (!isset($this->handles[$handle])) {
            throw new \Exception('Unknown handle');
        }

        if (!isset($this->tasks[$this->handles[$handle]])) {
            throw new \Exception('No task by that handle');
        }

        return $this->tasks[$this->handles[$handle]];
    }

    /**
     * Is this set finished running?
     *
     * This function will return true if all of the tasks in the set have
     * finished running. If they have we also run the set callbacks if there
     * is one.
     */
    public function finished(): bool {
        if (0 == $this->tasksCount) {
            if (isset($this->callback)) {
                foreach ($this->tasks as $task) {
                    $results[] = $task->result;
                }

                call_user_func($this->callback, $results);
            }

            return true;
        }

        return false;
    }

    /**
     * Attach a callback to this set.
     *
     * @param callable $callback A valid PHP callback
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function attachCallback(callable $callback): void {
        if (!is_callable($callback)) {
            throw new Exception('Invalid callback specified');
        }

        $this->callback = $callback;
    }

    /**
     * Get the iterator.
     *
     * @return ArrayIterator Tasks
     */
    public function getIterator(): \ArrayIterator {
        return new \ArrayIterator($this->tasks);
    }

    /**
     * Get the task count.
     *
     * @return int Number of tasks in the set
     */
    public function count(): int {
        return $this->tasksCount;
    }
}
