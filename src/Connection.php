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
 * The base connection class.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 *
 * @see      https://github.com/brianlmoon/net_gearman
 */
class Connection {
    /**
     * A list of valid Gearman commands.
     *
     * This is a list of valid Gearman commands (the key of the array), their
     * integery type (first key in second array) used in the binary header, and
     * the arguments / order of arguments to send/receive.
     *
     * @var array COMMANDS
     */
    protected const COMMANDS = [
        'all_yours'          => [24, []],
        'can_do'             => [1, ['func']],
        'can_do_timeout'     => [23, ['func', 'timeout']],
        'cant_do'            => [2, ['func']],
        'echo_req'           => [16, ['text']],
        'echo_res'           => [17, ['text']],
        'error'              => [19, ['err_code', 'err_text']],
        'get_status'         => [15, ['handle']],
        'grab_job'           => [9, []],
        'job_assign'         => [11, ['handle', 'func', 'arg']],
        'job_created'        => [8, ['handle']],
        'no_job'             => [10, []],
        'noop'               => [6, []],
        'pre_sleep'          => [4, []],
        'reset_abilities'    => [3, []],
        'set_client_id'      => [22, ['client_id']],
        'status_res'         => [20, ['handle', 'known', 'running', 'numerator', 'denominator']],
        'submit_job'         => [7, ['func', 'uniq', 'arg']],
        'submit_job_bg'      => [18, ['func', 'uniq', 'arg']],
        'submit_job_high'    => [21, ['func', 'uniq', 'arg']],
        'submit_job_high_bg' => [32, ['func', 'uniq', 'arg']],
        'submit_job_low'     => [33, ['func', 'uniq', 'arg']],
        'submit_job_low_bg'  => [34, ['func', 'uniq', 'arg']],
        'work_complete'      => [13, ['handle', 'result']],
        'work_fail'          => [14, ['handle']],
        'work_status'        => [12, ['handle', 'numerator', 'denominator']],
    ];

    /**
     * The reverse of Moonspot\Gearman\Connection::COMMANDS.
     *
     * This is the same as the Moonspot\Gearman\Connection::COMMANDS array only
     * it's keyed by the magic (integer value) value of the command.
     *
     * @var array
     */
    protected const MAGIC = [
        1  => ['can_do', ['func']],
        2  => ['cant_do', ['func']],
        3  => ['reset_abilities', []],
        4  => ['pre_sleep', []],
        6  => ['noop', []],
        7  => ['submit_job', ['func', 'uniq', 'arg']],
        8  => ['job_created', ['handle']],
        9  => ['grab_job', []],
        10 => ['no_job', []],
        11 => ['job_assign', ['handle', 'func', 'arg']],
        12 => ['work_status', ['handle', 'numerator', 'denominator']],
        13 => ['work_complete', ['handle', 'result']],
        14 => ['work_fail', ['handle']],
        15 => ['get_status', ['handle']],
        16 => ['echo_req', ['text']],
        17 => ['echo_res', ['text']],
        18 => ['submit_job_bg', ['func', 'uniq', 'arg']],
        19 => ['error', ['err_code', 'err_text']],
        20 => ['status_res', ['handle', 'known', 'running', 'numerator', 'denominator']],
        21 => ['submit_job_high', ['func', 'uniq', 'arg']],
        22 => ['set_client_id', ['client_id']],
        23 => ['can_do_timeout', ['func', 'timeout']],
        24 => ['all_yours', []],
        32 => ['submit_job_high_bg', ['func', 'uniq', 'arg']],
        33 => ['submit_job_low', ['func', 'uniq', 'arg']],
        34 => ['submit_job_low_bg', ['func', 'uniq', 'arg']],
    ];

    /**
     * Socket object.
     */
    public ?Socket $socket = null;

    /**
     * Tasks waiting for a handle.
     *
     * Tasks are popped onto this queue as they're submitted so that they can
     * later be popped off of the queue once a handle has been assigned via
     * the job_created command.
     */
    protected array $waiting = [];

    /**
     * Gearmand Server Version.
     */
    protected string $serverVersion;

    /**
     * Constructs a new instance.
     *
     * @param ?string $host    The host
     * @param int     $timeout The timeout
     */
    public function __construct(?string $host = null, int $timeout = 250) {
        if ($host) {
            $this->connect($host, $timeout);
        }
    }

    public function __destruct() {
        $this->close();
    }

    /**
     * Connect to Gearman.
     *
     * Opens the socket to the Gearman Job server. It throws an exception if
     * a socket error occurs.
     *
     * @param string $host    e.g. 127.0.0.1 or 127.0.0.1:7003
     * @param int    $timeout Timeout in milliseconds
     *
     * @throws Moonspot\Gearman\Exception when it can't connect to server
     */
    public function connect(string $host, int $timeout = 250): void {
        $this->close();

        if (strpos($host, ':')) {
            [$host, $port] = explode(':', $host);
        } else {
            $port = 4730;
        }

        $this->socket = null;

        $socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);

        $socket_connected = false;

        if (false !== $socket) {
            $this->socket = $socket;

            /*
             * Set the send and receive timeouts super low so that socket_connect
             * will return to us quickly. We then loop and check the real timeout
             * and check the socket error to decide if its connected yet or not.
             */
            socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => 0, 'usec' => 100]);
            socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => 0, 'usec' => 100]);

            // Explicitly set this to blocking which should be the default
            socket_set_block($this->socket);

            $now = microtime(true);
            $waitUntil = $now + $timeout / 1000;

            /*
             * Loop calling socket_connect. As long as the error is 115 (in progress)
             * or 114 (already called) and our timeout has not been reached, keep
             * trying.
             */
            do {
                socket_clear_error($this->socket);
                $socket_connected = @socket_connect($this->socket, $host, $port);
                $err = @socket_last_error($this->socket);
            } while ((115 === $err || 114 === $err) && (microtime(true) < $waitUntil));

            $elapsed = microtime(true) - $now;

            /**
             * For some reason, socket_connect can return true even when it is
             * not connected. Make sure it returned true the last error is zero.
             */
            $socket_connected = $socket_connected && 0 === $err;
        }

        if ($socket_connected) {
            socket_set_nonblock($this->socket);

            socket_set_option($this->socket, SOL_SOCKET, SO_KEEPALIVE, 1);

            /**
             * set the real send/receive timeouts here now that we are connected.
             */
            $timeout = self::calculateTimeout($timeout);
            socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => $timeout[0], 'usec' => $timeout[1]]);
            socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => $timeout[0], 'usec' => $timeout[1]]);

            // socket_set_option($this->socket, SOL_TCP, SO_DEBUG, 1); // Debug

            $this->setServerVersion($host);
        } else {
            $errno = @socket_last_error($this->socket);
            $errstr = @socket_strerror($errno);

            /*
             * close the socket just in case it
             * is somehow alive to some degree
             */
            $this->close();

            throw new Exception(
                "Can't connect to server ({$errno}: {$errstr})"
            );
        }

        $this->waiting = [];
    }

    /**
     * Adds a waiting task.
     *
     * @param Task $task The task
     */
    public function addWaitingTask(Task $task): void {
        $this->waiting[] = $task;
    }

    /**
     * Gets a waiting task.
     *
     * @return Task the waiting task
     */
    public function getWaitingTask(): Task {
        return array_shift($this->waiting);
    }

    /**
     * Send a command to Gearman.
     *
     * This is the command that takes the string version of the command you
     * wish to run (e.g. 'can_do', 'grab_job', etc.) along with an array of
     * parameters (in key value pairings) and packs it all up to send across
     * the socket.
     *
     * @param string $command Command to send (e.g. 'can_do')
     * @param array  $params  Params to send
     *
     * @throws Moonspot\Gearman\Exception on invalid command or unable to write
     */
    public function send(string $command, array $params = []): void {
        if (!isset($this::COMMANDS[$command])) {
            throw new Exception('Invalid command: '.$command);
        }

        if ('can_do_timeout' === $command) {
            $params = $this->fixTimeout($params);
        }

        $data = [];
        foreach ($this::COMMANDS[$command][1] as $field) {
            if (isset($params[$field])) {
                $data[] = $params[$field];
            }
        }

        $d = implode("\x00", $data);

        $cmd = "\0REQ".pack(
            'NN',
            $this::COMMANDS[$command][0],
            strlen($d)
        ).$d;

        $cmdLength = strlen($cmd);
        $written = 0;
        $error = false;
        do {
            $check = @socket_write(
                $this->socket,
                substr($cmd, $written, $cmdLength),
                $cmdLength
            );

            if (false === $check) {
                if (SOCKET_EAGAIN == socket_last_error($this->socket)
                    or SOCKET_EWOULDBLOCK == socket_last_error($this->socket)
                    or SOCKET_EINPROGRESS == socket_last_error($this->socket)) {
                    // skip this is okay
                } else {
                    $error = true;

                    break;
                }
            }

            $written += (int) $check;
        } while ($written < $cmdLength);

        if (true === $error) {
            $errno = @socket_last_error($this->socket);
            $errstr = @socket_strerror($errno);

            throw new Exception(
                "Could not write command to socket ({$errno}: {$errstr})"
            );
        }
    }

    /**
     * Read command from Gearman.
     *
     * @return array Result read back from Gearman
     *
     * @throws Moonspot\Gearman\Exception connection issues or invalid responses
     */
    public function read(): array {
        $header = '';
        do {
            $buf = @socket_read($this->socket, 12 - strlen($header));
            $header .= $buf;
        } while (false !== $buf
                 && '' !== $buf && strlen($header) < 12);

        if ('' === $buf) {
            throw new Exception('Connection was reset');
        }

        if (0 == strlen($header)) {
            return [];
        }
        $resp = @unpack('a4magic/Ntype/Nlen', $header);

        if (3 == !count($resp)) {
            throw new Exception('Received an invalid response');
        }

        if (!isset($this::MAGIC[$resp['type']])) {
            throw new Exception(
                'Invalid response magic returned: '.$resp['type']
            );
        }

        $return = [];
        if ($resp['len'] > 0) {
            $data = '';
            while (strlen($data) < $resp['len']) {
                $data .= @socket_read($this->socket, $resp['len'] - strlen($data));
            }

            $d = explode("\x00", $data);
            foreach ($this::MAGIC[$resp['type']][1] as $i => $a) {
                $return[$a] = $d[$i];
            }
        }

        $function = $this::MAGIC[$resp['type']][0];
        if ('error' == $function) {
            if (!strlen($return['err_text'])) {
                $return['err_text'] = 'Unknown error; see error code.';
            }

            throw new Exception("({$return['err_code']}): {$return['err_text']}");
        }

        return [
            'function' => $this::MAGIC[$resp['type']][0],
            'type'     => $resp['type'],
            'data'     => $return,
        ];
    }

    /**
     * Blocking socket read.
     *
     * @param int $timeout The timeout for the read in milliseconds
     *
     * @throws Moonspot\Gearman\Exception on timeouts
     */
    public function blockingRead(int $timeout = 250): array {
        $write = null;
        $except = null;
        $read = [$this->socket];

        $timeout = self::calculateTimeout($timeout);

        socket_clear_error($this->socket);
        $success = @socket_select($read, $write, $except, $timeout[0], $timeout[1]);
        if (false === $success) {
            $errno = @socket_last_error($this->socket);
            if (0 != $errno) {
                throw new Exception("Socket error: ({$errno}) ".socket_strerror($errno));
            }
        }

        if (0 === $success) {
            $errno = @socket_last_error($this->socket);

            throw new Exception(
                sprintf('Socket timeout (%.4fs, %.4fμs): (%s)', $timeout[0], $timeout[1], socket_strerror($errno))
            );
        }

        return $this->read();
    }

    /**
     * Close the connection.
     */
    public function close(): void {
        if (isset($this->socket) && is_resource($this->socket)) {
            socket_clear_error($this->socket);

            socket_set_block($this->socket);

            socket_set_option($this->socket, SOL_SOCKET, SO_SNDTIMEO, ['sec' => 0, 'usec' => 500]);
            socket_set_option($this->socket, SOL_SOCKET, SO_RCVTIMEO, ['sec' => 0, 'usec' => 500]);

            @socket_shutdown($this->socket);

            $err = socket_last_error($this->socket);
            if (0 != $err) {
                if (107 == $err) {
                    // 107 means Transport endpoint is not connected
                    unset($this->socket);

                    return;
                }

                throw new Exception("Socket error: ({$err}) ".socket_strerror($err));
            }

            /*
             * Read anything left on the buffer that we didn't get
             * due to a timeout or something
             */
            do {
                $err = 0;
                $buf = '';
                socket_clear_error($this->socket);
                socket_close($this->socket);
                if (isset($this->socket) && is_resource($this->socket)) {
                    $err = socket_last_error($this->socket);
                    // Check for EAGAIN error
                    // 11 on Linux
                    // 35 on BSD
                    if (11 == $err || 35 == $err) {
                        $buf = @socket_read($this->socket, 8192);
                        $err = socket_last_error($this->socket);
                    } else {
                        // Some other error was returned. We need to
                        // terminate the socket and get out. To do this,
                        // we set SO_LINGER to {on, 0} which causes
                        // the connection to be aborted.
                        socket_set_option(
                            $this->socket,
                            SOL_SOCKET,
                            SO_LINGER,
                            [
                                'l_onoff'  => 1,
                                'l_linger' => 0,
                            ]
                        );
                        socket_close($this->socket);
                        $err = 0;
                    }
                }
            } while (0 != $err && strlen($buf) > 0);

            unset($this->socket);
        }
    }

    /**
     * Are we connected?
     *
     * @return bool False if we aren't connected
     */
    public function isConnected(): bool {
        // PHP 8+ returns Socket object instead of resource
        if ($this->socket instanceof \Socket) {
            return true;
        }

        // PHP 5.x-7.x returns socket
        if (true === is_resource($this->socket)) {
            $type = strtolower(get_resource_type($this->socket));

            return 'socket' === $type;
        }

        return false;
    }

    /**
     * Calculates the timeout values for socket_select.
     *
     * @param int $milliseconds Timeout in milliseconds
     *
     * @return array The first value is the seconds and the second value
     *               is microseconds
     */
    public static function calculateTimeout(int $milliseconds): array {
        if ($milliseconds >= 1000) {
            $ts_seconds = $milliseconds / 1000;
            $tv_sec = floor($ts_seconds);
            $tv_usec = ($ts_seconds - $tv_sec) * 1000000;
        } else {
            $tv_sec = 0;
            $tv_usec = $milliseconds * 1000;
        }

        return [$tv_sec, $tv_usec];
    }

    /**
     * Sets the server version.
     *
     * @param string  $host    The host
     * @param Manager $manager Optional manager object
     */
    protected function setServerVersion(string $host, ?Manager $manager = null): void {
        if (empty($manager)) {
            $manager = new Manager($host);
        }
        $this->serverVersion = $manager->version();
        unset($manager);
    }

    /**
     * In gearmand version 1.1.19 and greater, the timeout is
     * expected to be in milliseconds. Before that version, it
     * is expected to be in seconds.
     * https://github.com/gearman/gearmand/issues/196.
     *
     * @param array $params The parameters
     */
    protected function fixTimeout(array $params): array {
        if (version_compare('1.1.18', $this->serverVersion)) {
            $params['timeout'] *= 1000;
        }

        return $params;
    }
}
