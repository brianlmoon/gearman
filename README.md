# Moonspot\Gearman

## About

Moonspot\Gearman is a package for interfacing with Gearman. Gearman is a system to farm out work to other machines, dispatching function calls to machines that are better suited to do work, to do work in parallel, to load balance lots of function calls, or to call functions between languages.

## Installation

```
$ composer require moonspot/gearman
```

## Examples

### Client

```
$client = new \Moonspot\Gearman\Client("localhost");
$set = new \Moonspot\Gearman\Set();
$task = new \Moonspot\Gearman\Task("Reverse_String", "foobar");
$task->attachCallback(
    function($func, $handle, $result){
        print_r($result)
    }
);
$set->addTask($task);
$client->runSet($set, $timeout);
```

### Worker

For easiest use, use GearmanManager for running workers. See: https://github.com/brianlmoon/GearmanManager

```
$worker = new \Moonspot\Gearman\Worker('localhost');
$worker->addAbility('Reverse_String');
$worker->beginWork();
```

### Job

```
class Reverse_String extends \Moonspot\Gearman\Job\Common {

    public function run($workload) {
        $result = strrev($workload);
        return $result;
    }
}
```
