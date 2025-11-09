<?php

namespace Moonspot\Gearman\Job;

use Moonspot\Gearman\Connection;

class TestJob extends Common
{
    private Connection $capturedConnection;
    private string $capturedHandle;
    private array $capturedParams;

    public function __construct(Connection $conn, string $handle, array $initParams = array())
    {
        parent::__construct($conn, $handle, $initParams);
        $this->capturedConnection = $conn;
        $this->capturedHandle = $handle;
        $this->capturedParams = $initParams;
    }

    public function run($arg)
    {
    }

    public function getCapturedConnection(): Connection
    {
        return $this->capturedConnection;
    }

    public function getCapturedHandle(): string
    {
        return $this->capturedHandle;
    }

    public function getCapturedParams(): array
    {
        return $this->capturedParams;
    }
}
