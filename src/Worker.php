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
 * @see      http://pear.php.net/package/Net_Gearman
 * @see      http://www.danga.com/gearman/
 */

/**
 * Gearman worker class.
 *
 * Run an instance of a worker to listen for jobs. It then manages the running
 * of jobs, etc.
 *
 * <code>
 * <?php
 *
 * $servers = array(
 *     '127.0.0.1:7003',
 *     '127.0.0.1:7004'
 * );
 *
 * $abilities = array('HelloWorld', 'Foo', 'Bar');
 *
 * try {
 *     $worker = new Worker($servers);
 *     foreach ($abilities as $ability) {
 *         $worker->addAbility('HelloWorld');
 *     }
 *     $worker->beginWork();
 * } catch (Exception $e) {
 *     echo $e->getMessage() . "\n";
 *     exit;
 * }
 *
 *
 * </code>
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @version   Release: @package_version@
 *
 * @see      http://www.danga.com/gearman/
 */
class Worker {
    /**
     * Callback type.
     *
     * @const integer JOB_START Runs when a job is started
     */
    public const JOB_START = 1;

    /**
     * Callback type.
     *
     * @const integer JOB_COMPLETE Runs when a job is finished
     */
    public const JOB_COMPLETE = 2;

    /**
     * Callback type.
     *
     * @const integer JOB_COMPLETE Runs when a job is finished
     */
    public const JOB_FAIL = 3;

    /**
     * Callback type.
     *
     * @const integer WORKER_STATUS Runs to send status info for servers
     */
    public const WORKER_STATUS = 4;

    /**
     * Pool of connections to Gearman servers.
     */
    protected array $conn = [];

    /**
     * Pool of retry connections.
     */
    protected array $retryConn = [];

    /**
     * List of servers that have failed a connection.
     */
    protected array $failedConn = [];

    /**
     * Holds a count of jobs done for each server.
     */
    protected array $stats = [];

    /**
     * Pool of worker abilities.
     */
    protected array $abilities = [];

    /**
     * Parameters for job contructors, indexed by ability name.
     */
    protected array $initParams = [];

    /**
     * Number of seconds to wait to retry a connection after it has failed.
     * If a server is already in the retry list when a new connection is
     * attempted, the retry time for that server will be increased using
     * this value as the base + a multiple.
     */
    protected float $retryTime = 3;

    /**
     * The maximum of amount of time in seconds to wait before trying to
     * reconnect to a server.
     */
    protected int $maxRetryTime = 60;

    /**
     * Stores the minimum current retry time of the servers in the retry list.
     * We use this when putting workers into pre_sleep so we can wake up
     * after this time and retry connections.
     */
    protected ?int $minCurrentRetryTime = null;

    /**
     * The time in seconds to to read on the sockets when we have had no work
     * to do. This prevents the worker from constantly requesting jobs
     * from the server.
     */
    protected int $sleepTime = 30;

    /**
     * Callbacks registered for this worker.
     *
     * @see Worker::JOB_START
     * @see Worker::JOB_COMPLETE
     * @see Worker::JOB_FAIL
     * @see Worker::WORKER_STATUS
     */
    protected array $callback = [
        self::JOB_START     => [],
        self::JOB_COMPLETE  => [],
        self::JOB_FAIL      => [],
        self::WORKER_STATUS => [],
    ];

    /**
     * Unique id for this worker.
     */
    protected string $id = '';

    /**
     * Socket timeout in milliseconds.
     */
    protected int $socket_timeout = 250;

    /**
     * Constructor.
     *
     * @param array  $servers        List of servers to connect to
     * @param string $id             Optional unique id for this worker
     * @param ?int   $socket_timeout Timout for the socket select
     *
     * @throws Exception
     *
     * @see Connection
     */
    public function __construct(array $servers, string $id = '', ?int $socket_timeout = null) {
        if (!is_array($servers) && strlen($servers)) {
            $servers = [$servers];
        } elseif (is_array($servers) && !count($servers)) {
            throw new Exception('Invalid servers specified');
        }

        if (empty($id)) {
            $id = 'pid_'.getmypid().'_'.uniqid();
        }

        $this->id = $id;

        if (!is_null($socket_timeout)) {
            if (is_numeric($socket_timeout)) {
                $this->socket_timeout = (int) $socket_timeout;
            } else {
                throw new Exception('Invalid valid for socket timeout');
            }
        }

        /*
         * Randomize the server list so all the workers don't try and connect
         * to the same server first causing a connection stampede
         */
        shuffle($servers);

        foreach ($servers as $s) {
            $this->connect($s);
        }
    }

    /**
     * Destructor.
     *
     * @see Worker::stop()
     */
    public function __destruct() {
        $this->endWork();
    }

    /**
     * Returns the status of the gearmand connections for this object.
     *
     * @return array An array containing a connected count, disconnected count
     *               and array that lists each server and true/false for connected
     */
    public function connection_status(): array {
        $servers = [];

        foreach ($this->conn as $server => $socket) {
            $servers[$server] = true;
        }
        foreach ($this->retryConn as $server => $status) {
            $servers[$server] = false;
        }

        return [
            'connected'    => count($this->conn),
            'disconnected' => count($this->retryConn),
            'servers'      => $servers,
            'stats'        => $this->stats,
        ];
    }

    /**
     * Announce an ability to the job server.
     *
     * @param string     $ability    Name of functcion/ability
     * @param ?int       $timeout    How long to give this job
     * @param array      $initParams Parameters for job constructor
     * @param Connection $conn       Optional connection to add ability to. if not set, all
     *                               connections are used
     *
     * @see $conn->send()
     */
    public function addAbility(string $ability, ?int $timeout = null, array $initParams = [], ?Connection $conn = null): void {
        $call = 'can_do';
        $params = ['func' => $ability];
        if (is_int($timeout) && $timeout > 0) {
            $params['timeout'] = $timeout;
            $call = 'can_do_timeout';
        }

        $this->initParams[$ability] = $initParams;

        $this->abilities[$ability] = $timeout;

        if ($conn) {
            $conn->send($call, $params);
        } else {
            foreach ($this->conn as $conn) {
                $conn->send($call, $params);
            }
        }
    }

    /**
     * Begin working.
     *
     * This starts the worker on its journey of actually working. The first
     * argument is a PHP callback to a function that can be used to monitor
     * the worker. If no callback is provided then the worker works until it
     * is killed. The monitor is passed two arguments; whether or not the
     * worker is idle and when the last job was ran. If the monitor returns
     * true, then the worker will stop working.
     *
     * @param ?callable $monitor Function to monitor work
     */
    public function beginWork(?callable $monitor = null): void {
        if (!is_callable($monitor)) {
            $monitor = [$this, 'stopWork'];
        }

        $keep_working = true;
        $lastJobTime = time();

        while ($keep_working) {
            $worked = false;

            $this->retryConnections();

            if (!empty($this->conn)) {
                $worked = $this->askForWork();

                if ($worked) {
                    $lastJobTime = time();
                }
            }

            if ($this->retryConnections()) {
                $sleep = false;
            } else {
                $sleep = !$worked;
            }

            if ($sleep && !empty($this->conn)) {
                $this->waitQuietly($monitor, $lastJobTime);
            }

            if (empty($this->conn)) {
                $this->deepSleep($monitor, $lastJobTime);
            }

            $keep_working = !call_user_func($monitor, !$worked, $lastJobTime);
        }
    }

    /**
     * Attach a callback.
     *
     * @param callable $callback A valid PHP callback
     * @param int      $type     Type of callback
     *
     * @throws Exception when an invalid callback is specified
     * @throws Exception when an invalid type is specified
     */
    public function attachCallback(callable $callback, int $type = self::JOB_COMPLETE): void {
        if (!is_callable($callback)) {
            throw new Exception('Invalid callback specified');
        }
        if (!isset($this->callback[$type])) {
            throw new Exception('Invalid callback type specified.');
        }
        $this->callback[$type][] = $callback;
    }

    /**
     * Stop working.
     */
    public function endWork(): void {
        foreach ($this->conn as $server => $conn) {
            $this->close($server);
        }
    }

    /**
     * Should we stop work?
     */
    public function stopWork(): bool {
        return false;
    }

    /**
     * Announce all abilities to all servers or one server.
     *
     * @param Connection $conn Optional connection to add ability to. if not set, all
     *                         connections are used
     */
    protected function addAbilities(?Connection $conn = null) {
        foreach ($this->abilities as $ability => $timeout) {
            $this->addAbility(
                $ability,
                $timeout,
                $this->initParams[$ability],
                $conn
            );
        }
    }

    /**
     * Monitors the sockets for incoming data which should cause an
     * immediate wake to perform work.
     *
     * @param callable $monitor     Callback to call for monitoring work
     * @param int      $lastJobTime The last job time
     */
    protected function waitQuietly(callable $monitor, int $lastJobTime): bool {
        // This is sent to notify the server that the worker is about to
        // sleep, and that it should be woken up with a NOOP packet if a
        // job comes in for a function the worker is able to perform.
        foreach ($this->conn as $server => $conn) {
            try {
                $conn->send('pre_sleep');
            } catch (Exception $e) {
                $this->sleepConnection($server);
            }
        }

        $this->status(
            'Worker going quiet for '.$this->sleepTime.' seconds'
        );

        $idle = true;
        $write = null;
        $except = null;

        $wakeTime = time() + $this->sleepTime;

        $socket_timeout = Connection::calculateTimeout($this->socket_timeout);

        while ($idle && $wakeTime > time()) {
            if (!empty($this->conn)) {
                foreach ($this->conn as $conn) {
                    $read_conns[] = $conn->socket;
                    socket_clear_error($conn->socket);
                }

                $success = @socket_select($read_conns, $write, $except, $socket_timeout[0], $socket_timeout[1]);

                if (call_user_func($monitor, true, $lastJobTime)) {
                    break;
                }

                // check for errors on any sockets
                if (false === $success) {
                    foreach ($this->conn as $server => $conn) {
                        $errno = socket_last_error($conn->socket);
                        if ($errno > 0) {
                            $this->status(
                                "Error while listening for wake up; Socket error ({$errno}): ".socket_strerror($errno),
                                $server
                            );
                            $this->sleepConnection($server);
                        }
                    }
                }

                // if we have any read connections left
                // after the socket_select call, then there
                // is work to do and we need to break
                $idle = empty($read_conns);
            }
        }

        return !$idle;
    }

    /**
     * If we have no open connections, sleep for the retry time. We don't
     * actually want to call sleep() for the whole time as the process will
     * not respond to signals. So, we will loop and sleep for 1s until the
     * retry time has passed.
     *
     * @param callable $monitor     Callback to call for monitoring work
     * @param int      $lastJobTime The last job time
     */
    protected function deepSleep(callable $monitor, int $lastJobTime): void {
        $retryTime = !empty($this->minCurrentRetryTime) ? $this->minCurrentRetryTime : $this->retryTime;

        $this->status(
            'No open connections. Sleeping for '.$retryTime.' seconds'
        );

        $now = time();
        do {
            sleep(1);
            if (call_user_func($monitor, true, $lastJobTime)) {
                break;
            }
        } while (microtime(true) - $now < $retryTime);
    }

    /**
     * Asks each server for work and performs any work that is sent.
     *
     * @param ?callable $monitor Callback to call for monitoring work
     * @param      "int       $lastJobTime  The last job time
     *
     * @return bool True if any work was done, false if not
     */
    protected function askForWork(?callable $monitor = null, ?int $lastJobTime = null) {
        $workDone = false;

        /**
         * Shuffle the list so we are not always starting with the same
         * server on every loop through the while loop.
         *
         * shuffle() destroys keys, so we have to loop a shuffle of the
         * keys.
         */
        $servers = array_keys($this->conn);
        shuffle($servers);

        foreach ($servers as $server) {
            $conn = $this->conn[$server];

            $worked = false;

            try {
                $this->status(
                    "Asking {$server} for work",
                    $server
                );

                $worked = $this->doWork($conn);

                if ($worked) {
                    $workDone = true;
                    if (empty($this->stats[$server])) {
                        $this->stats[$server] = 0;
                    }
                    ++$this->stats[$server];
                }
            } catch (Exception $e) {
                $this->status(
                    'Exception caught while doing work: '.$e->getMessage(),
                    $server
                );

                $this->sleepConnection($server);
            }

            if ($monitor && call_user_func($monitor, true, $lastJobTime)) {
                break;
            }
        }

        return $workDone;
    }

    /**
     * Attempts to reconnect to servers which are in a failed state.
     *
     * @return bool True if new connections were created, false if not
     */
    protected function retryConnections(): bool {
        $newConnections = false;

        if (count($this->retryConn)) {
            $now = time();

            foreach ($this->retryConn as $server => $retryTime) {
                if ($retryTime <= $now) {
                    $this->status(
                        "Attempting to reconnect to {$server}",
                        $server
                    );

                    // If we reconnect to a server, don't sleep
                    if ($this->connect($server)) {
                        $newConnections = true;
                    }
                }
            }

            // reset the min retry time as needed
            if (empty($this->retryConn)) {
                $this->minCurrentRetryTime = null;
            } else {
                $this->minCurrentRetryTime = min(array_values($this->retryConn)) - time();
            }
        }

        return $newConnections;
    }

    /**
     * Calculates the connection retry timeout.
     *
     * @param int $retry_count The number of times the connection has been retried
     *
     * @return int The number of seconds to wait before retrying
     */
    protected function retryTime(int $retry_count): int {
        return min($this->maxRetryTime, $this->retryTime << ($retry_count - 1));
    }

    /**
     * Listen on the socket for work.
     *
     * Sends the 'grab_job' command and then listens for either the 'noop' or
     * the 'no_job' command to come back. If the 'job_assign' comes down the
     * pipe then we run that job.
     *
     * @param Connection $conn Connection object
     *
     * @return bool Returns true if work was done, false if not
     *
     * @throws Exception
     */
    protected function doWork(Connection $conn): bool {
        $conn->send('grab_job');

        $resp = ['function' => 'noop'];
        while (count($resp) && 'noop' == $resp['function']) {
            $resp = $conn->blockingRead();
        }

        /*
         * The response can be empty during shut down. We don't need to proceed
         * in those cases. But, most of the time, it should not be.
         */
        if (!is_array($resp) || empty($resp)) {
            foreach ($this->conn as $s => $this_conn) {
                if ($conn == $this_conn) {
                    $server = $s;

                    break;
                }
            }

            $this->sleepConnection($server);

            $this->status(
                'No job was returned from the server',
                $server
            );

            return false;
        }

        if (in_array($resp['function'], ['noop', 'no_job'])) {
            return false;
        }

        if ('job_assign' != $resp['function']) {
            throw new Exception('Holy Cow! What are you doing?!');
        }

        $name = $resp['data']['func'];
        $handle = $resp['data']['handle'];
        $arg = [];

        if (isset($resp['data']['arg'])
            && Connection::stringLength($resp['data']['arg'])) {
            $arg = json_decode($resp['data']['arg'], true);
            if (null === $arg) {
                $arg = $resp['data']['arg'];
            }
        }

        try {
            if (empty($this->initParams[$name])) {
                $this->initParams[$name] = [];
            }

            $job = Job::factory(
                $name,
                $conn,
                $handle,
                $this->initParams[$name]
            );

            $this->start($handle, $name, $arg);
            $res = $job->run($arg);

            if (!is_array($res)) {
                $res = ['result' => $res];
            }

            $job->complete($res);
            $this->complete($handle, $name, $res);
        } catch (Job_Exception $e) {
            // If the factory method call fails, we won't have a job.
            if (isset($job) && $job instanceof Job\Common) {
                $job->fail();
            }

            $this->fail($handle, $name, $e);
        }

        // Force the job's destructor to run
        $job = null;

        return true;
    }

    /**
     * Run the job start callbacks.
     *
     * @param string $handle The job's Gearman handle
     * @param string $job    The name of the job
     * @param mixed  $args   The job's argument list
     */
    protected function start(string $handle, string $job, mixed $args): void {
        if (0 == count($this->callback[self::JOB_START])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_START] as $callback) {
            call_user_func($callback, $handle, $job, $args);
        }
    }

    /**
     * Run the complete callbacks.
     *
     * @param string $handle The job's Gearman handle
     * @param string $job    The name of the job
     * @param array  $result The job's returned result
     */
    protected function complete(string $handle, string $job, array $result): void {
        if (0 == count($this->callback[self::JOB_COMPLETE])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_COMPLETE] as $callback) {
            call_user_func($callback, $handle, $job, $result);
        }
    }

    /**
     * Run the fail callbacks.
     *
     * @param string $handle The job's Gearman handle
     * @param string $job    The name of the job
     * @param object $error  The exception thrown
     */
    protected function fail(string $handle, string $job, Exception $error): void {
        if (0 == count($this->callback[self::JOB_FAIL])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_FAIL] as $callback) {
            call_user_func($callback, $handle, $job, $error);
        }
    }

    /**
     * Run the worker status callbacks.
     *
     * @param string  $message a message about the worker's status
     * @param ?string $server  The server name related to the status
     */
    protected function status(string $message, ?string $server = null): void {
        if (0 == count($this->callback[self::WORKER_STATUS])) {
            return; // No callbacks to run
        }

        if (!empty($server)) {
            $failed_conns = $this->failedConn[$server] ?? 0;
            $connected = isset($this->conn[$server]) && $this->conn[$server]->isConnected();
        } else {
            $failed_conns = null;
            $connected = null;
        }

        foreach ($this->callback[self::WORKER_STATUS] as $callback) {
            call_user_func(
                $callback,
                $message,
                $server,
                $connected,
                $failed_conns
            );
        }
    }

    /**
     * Closes a connection to a server.
     *
     * @param string $server The server
     */
    protected function close(string $server): void {
        if (isset($this->conn[$server])) {
            $conn = $this->conn[$server];

            try {
                $conn->send('reset_abilities');
            } catch (Exception $e) {
            }
            $conn->close();
            unset($this->conn[$server]);
        }
    }

    /**
     * Connects to a gearman server and puts failed connections into the retry
     * list.
     *
     * @param string $server Server name/ip and optional port to connect
     */
    private function connect(string $server): bool {
        $success = false;

        try {
            /*
             * If this is a reconnect, be sure we close the old connection
             * before making a new one.
             */
            if (isset($this->conn[$server]) && is_resource($this->conn[$server])) {
                $this->close($server);
            }

            $this->conn[$server] = new Connection($server, $this->socket_timeout);

            $this->conn[$server]->send('set_client_id', ['client_id' => $this->id]);

            $this->addAbilities($this->conn[$server]);

            if (isset($this->retryConn[$server])) {
                unset($this->retryConn[$server]);
                $this->status('Removing server from the retry list.', $server);
            }

            $this->status("Connected to {$server}", $server);

            $success = true;
        } catch (Exception $e) {
            $this->sleepConnection($server);

            $this->status(
                'Connection failed',
                $server
            );
        }

        return $success;
    }

    /**
     * Removes a server from the connection list and adds it to a
     * reconnect list.
     *
     * @param string $server Server and port
     */
    private function sleepConnection(string $server): void {
        if (isset($this->conn[$server])) {
            $this->close($server);
        }

        if (empty($this->failedConn[$server])) {
            $this->failedConn[$server] = 1;
        } else {
            ++$this->failedConn[$server];
        }

        $waitTime = $this->retryTime($this->failedConn[$server]);
        $this->retryConn[$server] = time() + $waitTime;

        if (is_null($this->minCurrentRetryTime)) {
            $this->minCurrentRetryTime = $waitTime;
        } else {
            $this->minCurrentRetryTime = min(array_values($this->retryConn)) - time();
        }

        $this->status(
            "Putting {$server} connection to sleep for ".$waitTime.' seconds',
            $server
        );
    }
}
