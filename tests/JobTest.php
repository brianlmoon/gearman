<?php

namespace Moonspot\Gearman\Tests;

use Moonspot\Gearman\Connection;
use Moonspot\Gearman\Job;
use Moonspot\Gearman\Job\Exception as JobException;
use Moonspot\Gearman\Job\TestJob;
use PHPUnit\Framework\TestCase;

class JobTest extends TestCase
{
    public function testFactoryLoadsJobFromProvidedPath(): void
    {
        $conn = new Connection();
        $path = __DIR__ . '/Fixtures/Jobs/TestJob.php';

        $job = Job::factory('TestJob', $conn, 'H:1', array('path' => $path, 'custom' => 'value'));

        $this->assertInstanceOf(TestJob::class, $job);
        $this->assertSame($conn, $job->getCapturedConnection());
        $this->assertSame('H:1', $job->getCapturedHandle());
        $this->assertSame(array('path' => $path, 'custom' => 'value'), $job->getCapturedParams());
    }

    public function testFactoryAllowsCustomClassNameWithExplicitPath(): void
    {
        $conn = new Connection();
        $path = __DIR__ . '/Fixtures/AltJobs/CustomJob.php';

        $job = Job::factory(
            'IgnoredName',
            $conn,
            'H:2',
            array(
                'path' => $path,
                'class_name' => 'Custom\\Jobs\\CustomJob'
            )
        );

        $this->assertSame('Custom\\Jobs\\CustomJob', get_class($job));
    }

    public function testFactoryThrowsWhenJobFileMissing(): void
    {
        $this->expectException(JobException::class);
        $this->expectExceptionMessage('Invalid Job class file');

        Job::factory(
            'MissingJob',
            new Connection(),
            'H:3',
            array('path' => __DIR__ . '/Fixtures/Jobs/DoesNotExist.php')
        );
    }

    public function testFactoryThrowsWhenClassIsMissing(): void
    {
        $this->expectException(JobException::class);
        $this->expectExceptionMessage('Invalid Job class');

        Job::factory(
            'EmptyJobFile',
            new Connection(),
            'H:4',
            array('path' => __DIR__ . '/Fixtures/Jobs/EmptyJobFile.php')
        );
    }

    public function testFactoryThrowsWhenClassIsWrongType(): void
    {
        $this->expectException(JobException::class);
        $this->expectExceptionMessage('Job is of invalid type');

        Job::factory(
            'WrongTypeJob',
            new Connection(),
            'H:5',
            array('path' => __DIR__ . '/Fixtures/Jobs/WrongTypeJob.php')
        );
    }
}
