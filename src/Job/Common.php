<?php

namespace Moonspot\Gearman\Job;

use Moonspot\Gearman\Connection;

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
 * Base job class for all Gearman jobs.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 *
 * @version   Release: @package_version@
 *
 * @see      http://www.danga.com/gearman/
 * @see       Moonspot\Gearman\Job\Common, Moonspot\Gearman\Worker
 */
abstract class Common {
    /**
     * Gearman job handle.
     */
    protected string $handle = '';

    /**
     * Connection to Gearman.
     *
     * @var resource
     *
     * @see Moonspot\Gearman\Connection
     */
    protected Connection $conn;

    /**
     * Parameters for Job instantiation.
     */
    protected array $initParams = [];

    /**
     * Constructor.
     *
     * @param resource $conn       Connection to communicate with
     * @param string   $handle     Job ID / handle for this job
     * @param array    $initParams initialization parameters
     */
    public function __construct(Connection $conn, string $handle, array $initParams = []) {
        $this->conn = $conn;
        $this->handle = $handle;
        $this->initParams = $initParams;
    }

    /**
     * Run your job here.
     *
     * @param array $arg Arguments passed from the client
     *
     * @throws Moonspot\Gearman\Exception
     */
    abstract public function run(array $arg): mixed;

    /**
     * Update Gearman with your job's status.
     *
     * @param int $numerator   The numerator (e.g. 1)
     * @param int $denominator The denominator  (e.g. 100)
     *
     * @see Moonspot\Gearman\Connection::send()
     */
    public function status(int $numerator, int $denominator): void {
        $this->conn->send('work_status', [
            'handle'      => $this->handle,
            'numerator'   => $numerator,
            'denominator' => $denominator,
        ]);
    }

    /**
     * Mark your job as complete with its status.
     *
     * Net_Gearman communicates between the client and jobs in JSON. The main
     * benefit of this is that we can send fairly complex data types between
     * different languages. You should always pass an array as the result to
     * this function.
     *
     * NOTE: Your actual worker code should not call this if you are using the GearmanManager
     *
     * @param array $result Result of your job
     *
     * @see Moonspot\Gearman\Connection::send()
     */
    public function complete(array $result): void {
        $this->conn->send('work_complete', [
            'handle' => $this->handle,
            'result' => json_encode($result),
        ]);
    }

    /**
     * Mark your job as failing.
     *
     * If your job fails for some reason (e.g. a query fails) you need to run
     * this function and exit from your run() method. This will tell Gearman
     * (and the client by proxy) that the job has failed.
     *
     * NOTE: Your actual worker code should not call this if you are using
     * GearmanManager, you should throw a Moonspot\Gearman\Job\Exception instead.
     *
     * @see Moonspot\Gearman\Connection::send()
     */
    public function fail(): void {
        $this->conn->send('work_fail', [
            'handle' => $this->handle,
        ]);
    }
}
