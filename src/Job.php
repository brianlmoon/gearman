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
 * Job creation class.
 *
 * @category  Net
 *
 * @author    Joe Stump <joe@joestump.net>
 * @author    Brian Moon <brianm@dealnews.com>
 * @copyright 2007-2008 Digg.com, Inc.
 *
 * @see      https://github.com/brianlmoon/net_gearman
 */
abstract class Job {
    /**
     * Create an instance of a job.
     *
     * The Net_Geraman_Worker class creates connections to multiple job servers
     * and then fires off jobs using this function. It hands off the connection
     * which made the request for the job so that the job can communicate its
     * status from there on out.
     *
     * @param string     $job        Name of job (func in Gearman terms)
     * @param Connection $conn       Instance of Connection
     * @param string     $handle     Gearman job handle of job
     * @param string     $initParams initialisation parameters for job
     *
     * @see Job\Common
     *
     * @throws Exception
     */
    public static function factory(string $job, Connection $conn, string $handle, array $initParams = []): Job\Common {
        $file = null;
        if (empty($initParams['path'])) {
            if (defined('GEARMAN_JOB_PATH')) {
                $paths = explode(',', GEARMAN_JOB_PATH);

                foreach ($paths as $path) {
                    $tmpFile = $path.'/'.$job.'.php';

                    if (file_exists(realpath($tmpFile))) {
                        $file = $tmpFile;

                        break;
                    }
                }
            }
        } else {
            $file = $initParams['path'];
        }

        if (empty($initParams['class_name'])) {
            if (defined('GEARMAN_JOB_CLASS_PREFIX')) {
                $class = GEARMAN_JOB_CLASS_PREFIX;
            }
            $class .= $job;
        } else {
            $class = $initParams['class_name'];
        }

        if (!class_exists($class)) {
            if (!empty($file)) {
                if (!file_exists($file)) {
                    throw new Job_Exception('Invalid Job class file: '.(empty($file) ? '<empty>' : $file));
                }

                include_once $file;
            }
        }
        if (!class_exists($class)) {
            throw new Job_Exception('Invalid Job class: '.(empty($class) ? '<empty>' : $class).' in '.(empty($file) ? '<empty>' : $file));
        }

        $instance = new $class($conn, $handle, $initParams);
        if (!$instance instanceof Job\Common) {
            throw new Job_Exception('Job is of invalid type: '.get_class($instance));
        }

        return $instance;
    }
}
