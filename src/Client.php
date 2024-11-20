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
 * A client for submitting jobs to Gearman.
 *
 * This class is used by code submitting jobs to the Gearman server. It handles
 * taking tasks and sets of tasks and submitting them to the Gearman server.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 *
 * @see      https://github.com/brianlmoon/net_gearman
 */
class Client {
    /**
     * Constants for job priority.
     *
     * @var int
     */
    public const HIGH = 4;
    public const LOW = 6;
    public const NORMAL = 2;

    /**
     * Our randomly selected connection.
     *
     * @var resource An open socket to Gearman
     */
    protected array $conn = [];

    /**
     * A list of Gearman servers.
     *
     * @var array A list of potential Gearman servers
     */
    protected array $servers = [];

    /**
     * The timeout for Gearman connections.
     */
    protected int $timeout = 1000;

    /**
     * Callbacks array for receiving connection status.
     */
    protected mixed $callback;

    /**
     * Constructor.
     *
     * @param array $servers An array of servers or a single server
     * @param int   $timeout Timeout in miliseconds for the server connect time
     *                       If multiple servers have to be tried, the total
     *                       timeout for getConnection will be $timeout * {servers tried}
     *
     * @throws Exception
     *
     * @see Connection
     */
    public function __construct(array $servers, int $timeout = 1000) {
        if (!is_array($servers) && strlen($servers) > 0) {
            $servers = [$servers];
        } elseif (is_array($servers) && !count($servers)) {
            throw new Exception('Invalid servers specified');
        }

        $this->servers = array_values($servers);

        $this->timeout = $timeout;
    }

    /**
     * Destructor.
     */
    public function __destruct() {
        $this->disconnect();
    }

    /**
     * Fire off a background task with the given arguments.
     *
     * @param string $func Name of job to run
     * @param array  $args First key should be args to send
     *
     * @see Task, Set
     */
    public function __call(string $func, array $args = []): void {
        $send = '';
        if (isset($args[0]) && !empty($args[0])) {
            $send = $args[0];
        }

        $task = new Task($func, $send);
        $task->type = Task::JOB_BACKGROUND;

        $set = new Set();
        $set->addTask($task);
        $this->runSet($set);
    }

    /**
     * Attach a callback for connection status.
     *
     * @param callable $callback A valid PHP callback
     *
     * @throws Exception when an invalid callback is specified
     */
    public function attachCallback(callable $callback): void {
        if (!is_callable($callback)) {
            throw new Exception('Invalid callback specified');
        }
        $this->callback[] = $callback;
    }

    /**
     * Fire off a background task with the given arguments.
     *
     * @param string $func     The function
     * @param mixed  $payload  The payload
     * @param int    $priority The priority
     */
    public function run(string $func, mixed $payload, int $priority = self::NORMAL): string {
        $task = new Task($func, $payload);
        $task->type = Task::JOB_BACKGROUND;

        $set = new Set();
        $set->addTask($task);
        $this->runSet($set);

        return $task->handle;
    }

    /**
     * Run a set of tasks.
     *
     * @param object $set     A set of tasks to run
     * @param int    $timeout Time in seconds for the socket timeout. Max is 10 seconds
     *
     * @see Set, Task
     */
    public function runSet(Set $set, ?int $timeout = null): void {
        $totalTasks = $set->tasksCount;
        $taskKeys = array_keys($set->tasks);
        $t = 0;

        if (null !== $timeout) {
            $socket_timeout = min(10, (int) $timeout);
        } else {
            $socket_timeout = 10;
        }

        while (!$set->finished()) {
            if (null !== $timeout) {
                if (empty($start)) {
                    $start = microtime(true);
                } else {
                    $now = microtime(true);

                    if ($now - $start >= $timeout) {
                        break;
                    }
                }
            }

            if ($t < $totalTasks) {
                $k = $taskKeys[$t];
                $this->submitTask($set->tasks[$k]);
                if (Task::JOB_BACKGROUND == $set->tasks[$k]->type
                    || Task::JOB_HIGH_BACKGROUND == $set->tasks[$k]->type
                    || Task::JOB_LOW_BACKGROUND == $set->tasks[$k]->type) {
                    $set->tasks[$k]->finished = true;
                    --$set->tasksCount;
                }

                ++$t;
            }

            $write = null;
            $except = null;
            $read_cons = [];

            foreach ($this->conn as $conn) {
                $read_conns[] = $conn->socket;
            }

            @socket_select($read_conns, $write, $except, $socket_timeout);

            $error_messages = [];

            foreach ($this->conn as $server => $conn) {
                $err = socket_last_error($conn->socket);
                // Error 11 is EAGAIN and is normal in non-blocking mode
                // Error 35 happens on macOS often enough to be annoying
                if ($err && 11 != $err && 35 != $err) {
                    $msg = socket_strerror($err);
                    [$remote_address, $remote_port] = explode(':', $server);
                    $error_messages[] = "socket_select failed: ({$err}) {$msg}; server: {$remote_address}:{$remote_port}";
                }
                socket_clear_error($conn->socket);
                $resp = $conn->read();
                if (count($resp)) {
                    $this->handleResponse($resp, $conn, $set);
                }
            }

            // if all connections threw errors, throw an exception
            if (count($error_messages) == count($this->conn)) {
                throw new Exception(implode('; ', $error_messages));
            }
        }
    }

    /**
     * Disconnect from Gearman.
     */
    public function disconnect(): void {
        if (!is_array($this->conn) || !count($this->conn)) {
            return;
        }

        foreach ($this->conn as $conn) {
            if (is_callable([$conn, 'close'])) {
                $conn->close();
            }
        }

        $this->conn = [];
    }

    /**
     * Creates a singleton instance of this class for reuse.
     *
     * @param array $servers An array of servers or a single server
     * @param int   $timeout Timeout in microseconds
     *
     * @return object
     */
    public static function getInstance(array $servers, int $timeout = 1000): Client {
        static $instances;

        $key = md5(json_encode($servers));

        if (!isset($instances[$key])) {
            $instances[$key] = new Client($servers, $timeout);
        }

        return $instances[$key];
    }

    /**
     * Get a connection to a Gearman server.
     *
     * @param string $uniq    The unique id of the job
     * @param array  $servers Optional list of servers to limit use
     *
     * @return Connection A connection to a Gearman server
     */
    protected function getConnection(?string $uniq = null, ?array $servers = null): Connection {
        $conn = null;

        $start = microtime(true);
        $elapsed = 0;

        if (is_null($servers)) {
            $servers = $this->servers;
        }

        $try_servers = $servers;

        /**
         * Keep a list of the servers actually tried for the error message.
         */
        $tried_servers = [];

        while (null === $conn && count($servers) > 0) {
            if (1 === count($servers)) {
                $key = key($servers);
            } elseif (null === $uniq) {
                $key = array_rand($servers);
            } else {
                $key = ord(substr(md5($uniq), -1)) % count($servers);
            }

            $server = $servers[$key];

            $tried_servers[] = $server;

            if (empty($this->conn[$server]) || !$this->conn[$server]->isConnected()) {
                $conn = null;
                $start = microtime(true);
                $e = null;

                try {
                    $conn = new Connection($server, $this->timeout);
                } catch (Exception $e) {
                    $conn = null;
                }

                if (!$conn || !$conn->isConnected()) {
                    $conn = null;
                    unset($servers[$key]);
                    // we need to rekey the array
                    $servers = array_values($servers);
                } else {
                    $this->conn[$server] = $conn;

                    break;
                }

                foreach ($this->callback as $callback) {
                    call_user_func(
                        $callback,
                        $server,
                        null !== $conn,
                        $this->timeout,
                        microtime(true) - $start,
                        $e
                    );
                }
            } else {
                $conn = $this->conn[$server];
            }

            $elapsed = microtime(true) - $start;
        }

        if (empty($conn)) {
            $message = 'Failed to connect to a Gearman server. Attempted to connect to '.implode(',', $tried_servers).'.';
            if (count($tried_servers) != count($try_servers)) {
                $message .= ' Not all servers were tried. Full server list is '.implode(',', $try_servers).'.';
            }

            throw new Exception($message);
        }

        return $conn;
    }

    /**
     * Submit a task to Gearman.
     *
     * @param object $task Task to submit to Gearman
     *
     * @see         Task, Client::runSet()
     */
    protected function submitTask(Task $task): void {
        switch ($task->type) {
            case Task::JOB_LOW:
                $type = 'submit_job_low';

                break;

            case Task::JOB_LOW_BACKGROUND:
                $type = 'submit_job_low_bg';

                break;

            case Task::JOB_HIGH_BACKGROUND:
                $type = 'submit_job_high_bg';

                break;

            case Task::JOB_BACKGROUND:
                $type = 'submit_job_bg';

                break;

            case Task::JOB_HIGH:
                $type = 'submit_job_high';

                break;

            default:
                $type = 'submit_job';

                break;
        }

        // if we don't have a scalar
        // json encode the data
        if (!is_scalar($task->arg)) {
            $arg = @json_encode($task->arg);
        } else {
            $arg = $task->arg;
        }

        $params = [
            'func' => $task->func,
            'uniq' => $task->uniq,
            'arg'  => $arg,
        ];

        if (!empty($task->servers)) {
            $servers = $task->servers;
        } else {
            $servers = null;
        }

        $conn = $this->getConnection($task->uniq, $servers);
        $conn->send($type, $params);

        $conn->addWaitingTask($task);
    }

    /**
     * Handle the response read in.
     *
     * @param array      $resp  The raw array response
     * @param Connection $conn  The Connection
     * @param object     $tasks The tasks being ran
     *
     * @throws Exception
     */
    protected function handleResponse(array $resp, Connection $conn, Set $tasks): void {
        if (isset($resp['data']['handle'])
            && 'job_created' != $resp['function']) {
            $task = $tasks->getTask($resp['data']['handle']);
        }

        switch ($resp['function']) {
            case 'work_complete':
                $tasks->tasksCount--;
                $task->complete(json_decode($resp['data']['result'], true));

                break;

            case 'work_status':
                $n = (int) $resp['data']['numerator'];
                $d = (int) $resp['data']['denominator'];
                $task->status($n, $d);

                break;

            case 'work_fail':
                $tasks->tasksCount--;
                $task->fail();

                break;

            case 'job_created':
                $task = $conn->getWaitingTask();
                $task->handle = $resp['data']['handle'];
                if (Task::JOB_BACKGROUND == $task->type) {
                    $task->finished = true;
                }
                $tasks->handles[$task->handle] = $task->uniq;

                break;

            case 'error':
                throw new Exception('An error occurred');

            default:
                throw new Exception(
                    'Invalid function '.$resp['function']
                );
        }
    }
}
