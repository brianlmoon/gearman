<?php

namespace Moonspot\Gearman\Job;

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
 * Exception class for Gearman jobs.
 *
 * Your Gearman jobs should throw this from their run() method if they run
 * into any kind of error.
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
class Exception extends \Moonspot\Gearman\Exception {
}
