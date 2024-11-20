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
 * @see      https://github.com/brianlmoon/net_gearman
 */

/**
 * A client for managing Gearmand servers.
 *
 * This class implements the administrative text protocol used by Gearman to do
 * a number of administrative tasks such as collecting stats on workers, the
 * queue, shutting down the server, version, etc.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @see      https://github.com/brianlmoon/net_gearman
 */
class Manager {
    /**
     * Connection to gearmand.
     *
     * @var resource
     *
     * @see Moonspot\Gearman\Manager::sendCommand()
     * @see Moonspot\Gearman\Manager::recvCommand()
     */
    protected mixed $conn = false;

    /**
     * The server is shutdown.
     *
     * We obviously can't send more commands to a server after it's been shut
     * down. This is set to true in Moonspot\Gearman\Manager::shutdown() and then
     * checked in Moonspot\Gearman\Manager::sendCommand().
     */
    protected bool $shutdown = false;

    /**
     * Constructor.
     *
     * @param string $server  Host and port (e.g. 'localhost:4730')
     * @param int    $timeout Connection timeout
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function __construct(?string $server = null, int $timeout = 5) {
        if (func_num_args() > 0) {
            if (strpos($server, ':')) {
                [$host, $port] = explode(':', $server);
            } else {
                $host = $server;
                $port = 4730;
            }

            $errCode = 0;
            $errMsg = '';
            $this->conn = @fsockopen($host, $port, $errCode, $errMsg, $timeout);
            if (false === $this->conn) {
                throw new Exception(
                    'Could not connect to '.$host.':'.$port
                );
            }
        }
    }

    /**
     * Destructor.
     */
    public function __destruct() {
        $this->disconnect();
    }

    /**
     * Get the version of Gearman running.
     */
    public function version(): string {
        $this->sendCommand('version');
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        return trim($res);
    }

    /**
     * Shut down Gearman.
     *
     * @param bool $graceful Whether it should be a graceful shutdown
     *
     * @see Moonspot\Gearman\Manager::sendCommand()
     * @see Moonspot\Gearman\Manager::checkForError()
     * @see Moonspot\Gearman\Manager::$shutdown
     */
    public function shutdown(bool $graceful = false): bool {
        $cmd = ($graceful) ? 'shutdown graceful' : 'shutdown';
        $this->sendCommand($cmd);
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        $this->shutdown = ('OK' == trim($res));

        return $this->shutdown;
    }

    /**
     * Get worker status and info.
     *
     * Returns the file descriptor, IP address, client ID and the abilities
     * that the worker has announced.
     *
     * @return array A list of workers connected to the server
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function workers(): array {
        $this->sendCommand('workers');
        $res = $this->recvCommand();

        return $this->parseWorkersResponse($res);
    }

    /**
     * Parses a 'workers' response payload.
     *
     * @param string $res Response payload from a `workers` command
     */
    public function parseWorkersResponse(string $res): array {
        $workers = [];
        $tmp = explode("\n", $res);
        foreach ($tmp as $t) {
            if (0 === strlen($t)) {
                continue;
            }

            $t = trim($t);

            if (preg_match('/^(.+?) (.+?) (.+?) :(.*)$/', $t, $matches)) {
                $abilities = trim($matches[4]);
                $workers[] = [
                    'fd'        => $matches[1],
                    'ip'        => $matches[2],
                    'id'        => $matches[3],
                    'abilities' => empty($abilities) ? [] : explode(' ', $abilities),
                ];
            }
        }

        return $workers;
    }

    /**
     * Set maximum queue size for a function.
     *
     * For a given function of job, the maximum queue size is adjusted to be
     * max_queue_size jobs long. A negative value indicates unlimited queue
     * size.
     *
     * If the max_queue_size value is not supplied then it is unset (and the
     * default maximum queue size will apply to this function).
     *
     * @param string $function Name of function to set queue size for
     * @param int    $size     New size of queue
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function setMaxQueueSize(srring $function, int $size): bool {
        if (!is_numeric($size)) {
            throw new Exception('Queue size must be numeric');
        }

        if (preg_match('/[^a-z0-9_]/i', $function)) {
            throw new Exception('Invalid function name');
        }

        $this->sendCommand('maxqueue '.$function.' '.$size);
        $res = fgets($this->conn, 4096);
        $this->checkForError($res);

        return 'OK' == trim($res);
    }

    /**
     * Get queue/worker status by function.
     *
     * This function queries for queue status. The array returned is keyed by
     * the function (job) name and has how many jobs are in the queue, how
     * many jobs are running and how many workers are capable of performing
     * that job.
     *
     * @return array An array keyed by function name
     *
     * @throws Moonspot\Gearman\Exception
     */
    public function status(): array {
        $this->sendCommand('status');
        $res = $this->recvCommand();

        $status = [];
        $tmp = explode("\n", $res);
        foreach ($tmp as $t) {
            if (!Moonspot\Gearman\Connection::stringLength($t)) {
                continue;
            }

            [$func, $inQueue, $jobsRunning, $capable] = explode("\t", $t);

            $status[$func] = [
                'in_queue'        => $inQueue,
                'jobs_running'    => $jobsRunning,
                'capable_workers' => $capable,
            ];
        }

        return $status;
    }

    /**
     * Disconnect from server.
     *
     * @see Moonspot\Gearman\Manager::$conn
     */
    public function disconnect(): void {
        if (is_resource($this->conn)) {
            fclose($this->conn);
        }
    }

    /**
     * Send a command.
     *
     * @param string $cmd The command to send
     *
     * @throws Moonspot\Gearman\Exception
     */
    protected function sendCommand(string $cmd): void {
        if ($this->shutdown) {
            throw new Exception('This server has been shut down');
        }

        fwrite(
            $this->conn,
            $cmd."\r\n",
            Moonspot\Gearman\Connection::stringLength($cmd."\r\n")
        );
    }

    /**
     * Receive a response.
     *
     * For most commands Gearman returns a bunch of lines and ends the
     * transmission of data with a single line of ".\n". This command reads
     * in everything until ".\n". If the command being sent is NOT ended with
     * ".\n" DO NOT use this command.
     *
     * @throws Moonspot\Gearman\Exception
     */
    protected function recvCommand(): string {
        $ret = '';
        while (true) {
            $data = fgets($this->conn, 4096);
            $this->checkForError($data);
            if (".\n" == $data) {
                break;
            }

            $ret .= $data;
        }

        return $ret;
    }

    /**
     * Check for an error.
     *
     * Gearman returns errors in the format of 'ERR code_here Message+here'.
     * This method checks returned values from the server for this error format
     * and will throw the appropriate exception.
     *
     * @param string $data The returned data to check for an error
     *
     * @throws Moonspot\Gearman\Exception
     */
    protected function checkForError(string $data): void {
        $data = trim($data);
        if (preg_match('/^ERR/', $data)) {
            [, $code, $msg] = explode(' ', $data);

            throw new Exception($msg.' [error code: '.urldecode($code).']');
        }
    }
}
