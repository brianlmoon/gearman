# Net Gearman

## About

Net_Gearman is a package for interfacing with Gearman. Gearman is a system to farm out work to other machines,
dispatching function calls to machines that are better suited to do work, to do work in parallel, to load balance lots
of function calls, or to call functions between languages.

## Installation

```
$ composer require brianlmoon/gearman
```

## Examples

### Client

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Moonspot\Gearman\Client;
use Moonspot\Gearman\Set;
use Moonspot\Gearman\Task;

$client = new Client('localhost');
$set = new Set();
$task = new Task('Reverse_String', 'foobar');
$task->attachCallback(
    function ($func, $handle, $result) {
        print_r($result);
    }
);
$set->addTask($task);
$client->runSet($set);
```

### Job

```php
<?php

namespace App\Gearman;

use Moonspot\Gearman\Job\Common;

class ReverseString extends Common
{
    public function run($workload)
    {
        return strrev($workload);
    }
}
```

### Worker

For easiest use, use GearmanManager for running workers. See: https://github.com/brianlmoon/GearmanManager

```php
<?php

require __DIR__ . '/vendor/autoload.php';

use Moonspot\Gearman\Worker;

$worker = new Worker('localhost');
$worker->addAbility('Reverse_String');
$worker->beginWork();
```

## Functional Tests

To run the functional tests, docker is required. Run a gearmand in docker with a command like:

```shell
docker run -d -p 4730:4730 --rm \
    --name gearmand \
    artefactual/gearmand:latest
```

Once that is running, run the tests with the functional group flag.

```shell
./vendor/bin/phpunit --group=functional
```
