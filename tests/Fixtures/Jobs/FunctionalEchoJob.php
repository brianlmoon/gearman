<?php

namespace Moonspot\Gearman\Job;

class FunctionalEchoJob extends Common
{
    public function run($arg)
    {
        if (!is_array($arg)) {
            $arg = array('raw' => $arg);
        }

        return array(
            'job' => 'FunctionalEchoJob',
            'payload' => $arg,
            'worker_pid' => getmypid(),
        );
    }
}
