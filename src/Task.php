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
 * Task class for creating Net_Gearman tasks.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @see      https://github.com/brianlmoon/net_gearman
 * @see       Moonspot\Gearman\Set, Moonspot\Gearman\Client
 */
class Task {
    /**
     * Normal job.
     *
     * Normal jobs are ran against a worker with the result being returned
     * all in the same thread (e.g. Your page will sit there waiting for the
     * job to finish and return it's result).
     *
     * @var int JOB_NORMAL
     */
    public const JOB_NORMAL = 1;

    /**
     * Background job.
     *
     * Background jobs in Gearman are "fire and forget". You can check a job's
     * status periodically, but you can't get a result back from it.
     *
     * @var int JOB_BACKGROUND
     */
    public const JOB_BACKGROUND = 2;

    /**
     * High priority job.
     *
     * @var int JOB_HIGH
     */
    public const JOB_HIGH = 3;

    /**
     * High priority, background job.
     *
     * @var int JOB_HIGH
     */
    public const JOB_HIGH_BACKGROUND = 4;

    /**
     * LOW priority job.
     *
     * @var int JOB_LOW
     */
    public const JOB_LOW = 5;

    /**
     * Low priority, background job.
     *
     * @var int JOB_LOW_BACKGROUND
     */
    public const JOB_LOW_BACKGROUND = 6;

    /**
     * Callback of type complete.
     *
     * The callback provided should be ran when the task has been completed. It
     * will be handed the result of the task as its only argument.
     *
     * @var int TASK_COMPLETE
     *
     * @see Moonspot\Gearman\Task::complete()
     */
    public const TASK_COMPLETE = 1;

    /**
     * Callback of type fail.
     *
     * The callback provided should be ran when the task has been reported to
     * have failed by Gearman. No arguments are provided.
     *
     * @var int TASK_FAIL
     *
     * @see Moonspot\Gearman\Task::fail()
     */
    public const TASK_FAIL = 2;

    /**
     * Callback of type status.
     *
     * The callback provided should be ran whenever the status of the task has
     * been updated. The numerator and denominator are passed as the only
     * two arguments.
     *
     * @var int TASK_STATUS
     *
     * @see Moonspot\Gearman\Task::status()
     */
    public const TASK_STATUS = 3;

    /**
     * The function/job to run.
     */
    public string $func = '';

    /**
     * Arguments to pass to function/job.
     */
    public array $arg = [];

    /**
     * Type of job.
     *
     * Which type of job you wish this task to be ran as. Keep in mind that
     * background jobs are "fire and forget" and DO NOT return results to the
     * job server in a manner that you can actually retrieve.
     *
     * @see Moonspot\Gearman\Task::JOB_NORMAL
     * @see Moonspot\Gearman\Task::JOB_BACKGROUND
     * @see Moonspot\Gearman\Task::JOB_HIGH
     * @see Moonspot\Gearman\Task::JOB_HIGH_BACKGROUND
     * @see Moonspot\Gearman\Task::JOB_LOW
     * @see Moonspot\Gearman\Task::JOB_LOW_BACKGROUND
     */
    public int $type = self::JOB_NORMAL;

    /**
     * Handle returned from job server.
     *
     * @see Moonspot\Gearman\Client
     */
    public string $handle = '';

    /**
     * List of servers this task can run on. If not set, the servers the client
     * has will be used. This is for cases where different tasks all in one
     * set may only be available on certain servers.
     */
    public array $servers = [];

    /**
     * Server used for the task.
     *
     * @see Moonspot\Gearman\Client
     */
    public string $server = '';

    /**
     * The unique identifier for this job.
     *
     * Keep in mind that a unique job is only unique to the job server it is
     * submitted to. Gearman servers don't communicate with each other to
     * ensure a job is unique across all workers.
     *
     * That being said, Gearman does group identical jobs sent to it and runs
     * that job only once. If you send the job Sum with args 1, 2, 3 to the
     * server 10 times in a second Gearman will only run that job once and then
     * return the result 10 times.
     */
    public string $uniq = '';

    /**
     * Is this task finished?
     *
     * @see Moonspot\Gearman\Set::finished()
     * @see Moonspot\Gearman\Task::complete()
     * @see Moonspot\Gearman\Task::fail()
     */
    public bool $finished = false;

    /**
     * The result returned from the worker.
     */
    public mixed $result = '';

    /**
     * Callbacks registered for each state.
     *
     * @see Moonspot\Gearman\Task::attachCallback()
     * @see Moonspot\Gearman\Task::complete()
     * @see Moonspot\Gearman\Task::status()
     * @see Moonspot\Gearman\Task::fail()
     */
    protected array $callback = [
        self::TASK_COMPLETE => [],
        self::TASK_FAIL     => [],
        self::TASK_STATUS   => [],
    ];

    /**
     * Constructor.
     *
     * @param string  $func Name of job to run
     * @param mixed   $arg  Arguments for job
     * @param ?string $uniq The unique id of the job
     * @param int     $type Type of job to run task as
     *
     * @return Moonspot\Gearman\Task
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function __construct(
        string $func,
        mixed $arg,
        ?string $uniq = null,
        int $type = self::JOB_NORMAL,
        array $servers = []
    ) {
        $this->func = $func;
        $this->arg = $arg;

        if (is_null($uniq)) {
            $this->uniq = md5($func.serialize($arg).$type);
        } else {
            $this->uniq = $uniq;
        }

        if (!empty($servers)) {
            $this->servers = $servers;
        }

        $this->type = $type;

        if (!in_array(
            $type,
            [self::JOB_NORMAL, self::JOB_BACKGROUND, self::JOB_HIGH,
                self::JOB_HIGH_BACKGROUND, self::JOB_LOW, self::JOB_LOW_BACKGROUND, ]
        )) {
            throw new Exception(
                "Unknown job type: {$type}. Please see Moonspot\\Gearman\\Task::JOB_* constants."
            );
        }

        $this->type = $type;
    }

    /**
     * Attach a callback to this task.
     *
     * @param callable $callback A valid PHP callback
     * @param int      $type     Type of callback
     *
     * @return $this
     *
     * @throws Moonspot\Gearman\Exception when the callback is invalid
     * @throws Moonspot\Gearman\Exception when the callback's type is invalid
     */
    public function attachCallback(callable $callback, int $type = self::TASK_COMPLETE): Task {
        if (!is_callable($callback)) {
            throw new Exception('Invalid callback specified');
        }

        if (!in_array(
            $type,
            [self::TASK_COMPLETE, self::TASK_FAIL, self::TASK_STATUS]
        )) {
            throw new Exception('Invalid callback type specified');
        }

        $this->callback[$type][] = $callback;

        return $this;
    }

    /**
     * Return all callbacks.
     */
    public function getCallbacks(): array {
        return $this->callback;
    }

    /**
     * Run the complete callbacks.
     *
     * Complete callbacks are passed the name of the job, the handle of the
     * job and the result of the job (in that order).
     *
     * @param mixed $result JSON decoded result passed back
     *
     * @see Moonspot\Gearman\Task::attachCallback()
     */
    public function complete(mixed $result): void {
        $this->finished = true;
        $this->result = $result;

        if (!count($this->callback[self::TASK_COMPLETE])) {
            return;
        }

        foreach ($this->callback[self::TASK_COMPLETE] as $callback) {
            call_user_func($callback, $this->func, $this->handle, $result, $this->uniq);
        }
    }

    /**
     * Run the failure callbacks.
     *
     * Failure callbacks are passed the task object job that failed:
     * <code>
     * // example callback
     * function failCallback(Moonspot\Gearman\Task $task) {
     *     var_dump($task);
     * }
     * $task->attachCallback('failCallback', Moonspot\Gearman\Task::TASK_FAIL);
     * </code>
     *
     * @see    Moonspot\Gearman\Task::attachCallback()
     */
    public function fail(): void {
        $this->finished = true;
        if (!count($this->callback[self::TASK_FAIL])) {
            return;
        }

        foreach ($this->callback[self::TASK_FAIL] as $callback) {
            call_user_func($callback, $this);
        }
    }

    /**
     * Run the status callbacks.
     *
     * Status callbacks are passed the name of the job, handle of the job and
     * the numerator/denominator as the arguments (in that order).
     *
     * @param int $numerator   The numerator from the status
     * @param int $denominator The denominator from the status
     *
     * @see Moonspot\Gearman\Task::attachCallback()
     */
    public function status(int $numerator, int $denominator): void {
        if (!count($this->callback[self::TASK_STATUS])) {
            return;
        }

        foreach ($this->callback[self::TASK_STATUS] as $callback) {
            call_user_func(
                $callback,
                $this->func,
                $this->handle,
                $numerator,
                $denominator
            );
        }
    }
}
