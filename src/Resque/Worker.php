<?php

/*
 * This file is part of the php-resque package.
 *
 * (c) Michael Haynes <mike@mjphaynes.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Resque;

use Resque\Helpers\Stats;

/**
 * Resque worker class
 *
 * @author Michael Haynes <mike@mjphaynes.com>
 */
class Worker
{

    /**
     * New worker constant
     */
    const STATUS_NEW = 1;

    /**
     * Running worker constant
     */
    const STATUS_RUNNING = 2;

    /**
     * Paused worker constant
     */
    const STATUS_PAUSED = 3;

    /**
     * Worker status constants as text
     */
    public static $statusText = array(
        self::STATUS_NEW     => 'Not started',
        self::STATUS_RUNNING => 'Running',
        self::STATUS_PAUSED  => 'Paused'
    );

    /**
     * Host timeout cleanup cycle
     */
    const CLEANUP_CYCLE = 120;

    /**
     * The Redis instance.
     *
     * @var Redis
     */
    protected $redis;

    /**
     * @var array Array of all associated queues for this worker.
     */
    protected $queues = array();

    /**
     * @var Host The host of this worker.
     */
    protected $host;

    /**
     * @var bool True if on the next iteration, the worker should shutdown.
     */
    protected $shutdown = false;

    /**
     * @var bool Status of the worker.
     */
    protected $status = self::STATUS_NEW;

    /**
     * @var string String identifying this worker.
     */
    protected $id;

    /**
     * @var int Process id of this worker
     */
    protected $pid;

    /**
     * @var string File to store process id in
     */
    protected $pidFile = null;

    /**
     * @var Job Current job, if any, being processed by this worker.
     */
    protected $job = null;

    /**
     * @var int Process ID of child worker processes.
     */
    protected $child = null;

    /**
     * @var bool True if uses Redis pop blocking
     */
    protected $blocking = true;

    /**
     * @var int Clock speed
     */
    protected $interval = 10;

    /**
     * @var int Max execution time of job
     */
    protected $timeout = 60;

    /**
     * @var int Memory limit of worker, if exceeded worker will stop
     */
    protected $memoryLimit = 128;

    /**
     * Dedicated lock, used when maintenance, only dedicated bot can run jobs
     * @var boolean
     */
    protected $dedicatedLock = false;

    /**
     * @var array List of shutdown errors to catch
     */
    protected $shutdownErrors = array(
        E_PARSE,
        E_ERROR,
        E_USER_ERROR,
        E_CORE_ERROR,
        E_CORE_WARNING,
        E_COMPILE_ERROR,
        E_COMPILE_WARNING
    );

    /**
     * @var Logger logger instance
     */
    protected $logger = null;

    /**
     * Incapabilities
     * @var array
     */
    protected $incapabilities = array();

    /**
     * Capabilities
     *
     * @var array
     **/
    protected $capabilities = array();

    /**
     * Get the Redis key
     *
     * @param  Worker $worker the worker to get the key for
     * @param  string $suffix to be appended to key
     * @return string
     */
    public static function redisKey($worker = null, $suffix = null)
    {
        if (is_null($worker)) {
            return 'workers';
        }

        $id = $worker instanceof Worker ? $worker->id : $worker;

        return 'worker:'.$id.($suffix ? ':'.$suffix : '');
    }

    /**
     * Return a worker from it's ID
     *
     * @param  string $id     Worker id
     * @param  Logger $logger Logger for the worker to use
     * @return Worker
     */
    public static function fromId($id, Logger $logger = null)
    {
        if (!$id or !count($packet = Redis::instance()->hgetall(self::redisKey($id)))) {
            return false;
        }

        if(!$packet) {
            return false;
        }

        $worker = new static(explode(',', $packet['queues']), $packet['blocking']);
        $worker->setId($id);
        $worker->setPid($packet['pid']);
        $worker->setInterval($packet['interval']);
        $worker->setTimeout($packet['timeout']);
        $worker->setMemoryLimit($packet['memory_limit']);
        $worker->setHost(new Host($packet['hostname']));
        $worker->shutdown = isset($packet['shutdown']) ? $packet['shutdown'] : null;
        $worker->setLogger($logger);

        return $worker;
    }

    /**
     * Create a new worker.
     *
     * @param mixed $queues   Queues for the worker to watch
     * @param bool  $blocking Use Redis blocking
     */
    public function __construct($queues = '*', $blocking = true)
    {
        $this->redis    = Redis::instance();
        $this->queues   = array_map('trim', is_array($queues) ? $queues : explode(',', $queues));
        $this->blocking = (bool)$blocking;

        $this->host = new Host();
        $this->pid  = getmypid();
        $this->id   = $this->host. ':' . $this->pid . ':' . phpversion();
        $this->dedicatedLock = false;

        Event::fire(Event::WORKER_INSTANCE, $this);
    }

    public static function setMaintenance() {
        return(Redis::instance())->hset("global",  "dedicated", "maintenance");
    }

    public static function removeDedicated() {
        (Redis::instance())->hset("global",  "cluster", uniqid());
        return (Redis::instance())->hdel("global",  "dedicated");
    }


    /**
     * Set worker to dedicated mode
     *
     * @return boolean
     * @author 
     **/
    public function setDedicated()
    {
        return $this->redis->hset("global", "dedicated", $this->getId());
    }

    public function handleRemoteSignal() {

        if($this->job) {
            $packet = $this->job->getPacket();
            $overrideCancel = (
                isset($packet['override_status']) && 
                $packet['override_status'] == Job::STATUS_CANCELLED
            );
            if($overrideCancel) {
                $this->log('Job appears cancelled.', Logger::INFO);
                $this->cancelJob();
                $signalData = 'CANCEL';
            }
        }

        $dedicated = $this->redis->hget("global", "dedicated");
        if(!empty($dedicated) && $this->getId() != $dedicated) {
            $this->dedicatedLock = true;
            $this->log('Dedicated mode, this worker ' . $this->getId() . ' is not eligible to work currently, sleep for a while. <pop>'.$dedicated.'</pop>', Logger::INFO);
            $this->sigPause();
        } else if ($this->dedicatedLock) {
            $this->sigResume();
            $this->dedicatedLock = false;
        }
        $globalData = $this->redis->hget("global", "signal");
        if(!empty($globalData)) {
            $lastGSignal = $this->redis->hget(self::redisKey($this), 'last_g_signal');
            if($lastGSignal != $globalData) {
                $signalData = $globalData;
                $this->redis->hset(self::redisKey($this), 'last_g_signal', $globalData);
            }
        }
        if(empty($signalData)) {
            $signalData = $this->redis->hget(self::redisKey($this), 'signal');
            $this->redis->hdel(self::redisKey($this), 'signal');
        }
        if($signalData) {
            $this->log('Remote signal: <pop>'.$signalData.'</pop>', Logger::INFO);
        } else {
            return;
        }
        if($signalData) {
            switch(strtoupper($signalData)) {
                case "FORCESHUTDOWN":
                case "SIGTERM":
                case "SIGINT":
                posix_kill(getmypid(), SIGKILL);
                break;
                case "QUIT":
                case "SHUTDOWN":
                case "SIGQUIT":
                posix_kill(getmypid(), SIGQUIT);
                break;
                case "CANCEL":
                case "SIGUSR1": 
                posix_kill(getmypid(), SIGUSR1);
                break;
                case "PAUSE":
                case "SIGUSR2":
                posix_kill(getmypid(), SIGUSR2);
                break;
                case "RESUME":
                case "SIGCONT":
                posix_kill(getmypid(), SIGCONT);
                break;
                case "SIGPIPE":
                posix_kill(getmypid(), SIGPIPE);
                break;
                default:
                $this->log('Unhandled <pop>'.$signalData.'</pop>', Logger::INFO);
                break;
            }
        };
    }

    /**
     * Generate a string representation of this worker.
     *
     * @return string String identifier for this worker instance.
     */
    public function __toString()
    {
        return $this->id;
    }

    /**
     * The primary loop for a worker which when called on an instance starts
     * the worker's life cycle.
     *
     * Queues are checked every $interval (seconds) for new jobs.
     */
    public function work()
    {
        $this->log('Starting worker <pop>'.$this.'</pop>', Logger::INFO);
        $this->updateProcLine('Worker: starting...');

        $this->startup();

        $this->log('Listening to queues: <pop>'.implode(', ', $this->queues).'</pop>, with '.
            ($this->blocking ? 'timeout blocking' : 'time interval').' <pop>'.$this->interval_string().'</pop>', Logger::INFO);

        $lastResetTime = time();

        while (true) {

            if((time() - $lastResetTime) >= self::CLEANUP_CYCLE) {
                $lastResetTime = time();
                $this->host->cleanup();
                $this->cleanup();
                $oldHostname = $this->host;
                $this->host->updateHostname();
                if($this->host != $oldHostname) {
                    return $this->shutdown();
                }
            }

            $this->handleRemoteSignal();

            if ($this->memoryExceeded()) {
                $this->log('Worker memory has been exceeded, aborting', Logger::CRITICAL);
                $this->shutdown();

                Event::fire(Event::WORKER_LOW_MEMORY, $this);
            }

            if (!$this->redis->sismember(self::redisKey(), $this->id) or $this->redis->hlen(self::redisKey($this)) == 0) {
                $this->log('Worker ' . $this->id . ' is not in list of workers or packet is corrupt, aborting', Logger::CRITICAL);
                $this->shutdown();

                Event::fire(Event::WORKER_CORRUPT, $this);
            }

            $this->shutdown = $this->redis->hget(self::redisKey($this), 'shutdown');

            if ($this->shutdown) {
                $this->log('Shutting down worker <pop>'.$this.'</pop>', Logger::INFO);
                $this->updateProcLine('Worker: shutting down...');
                break;
            }

            $this->host->working($this);

            if ($this->status == self::STATUS_PAUSED) {
                $this->log('Worker paused, trying again in '.$this->interval_string(), Logger::INFO);
                $this->updateProcLine('Worker: paused');
                sleep($this->interval);
                continue;
            }

            $this->redis->hmset(self::redisKey($this), 'memory', memory_get_usage());


            Event::fire(Event::WORKER_WORK, $this);

            if (!count($this->resolveQueues())) {
                $this->log('No queues found, waiting for '.$this->interval_string(), Logger::INFO);
                sleep($this->interval);
                continue;
            }

            $this->queueDelayed();

            if ($this->blocking) {
                $this->log('[' . $this->getId() . '] Pop blocking with timeout of '.$this->interval_string() . ' q: ' . implode(',', $this->resolveQueues()), Logger::DEBUG);
                $this->updateProcLine('Worker: waiting for job on '.implode(',', $this->queues).' with blocking timeout '.$this->interval_string());
            } else {
                $this->updateProcLine('Worker: waiting for job on '.implode(',', $this->queues).' with interval '.$this->interval_string());
            }

            $job = \Resque::pop($this->resolveQueues(), $this->interval, $this->blocking, $this);

            if (!$job instanceof Job) {
                if (!$this->blocking) {
                    $this->log('Sleeping for '.$this->interval_string(), Logger::DEBUG);
                    sleep($this->interval);
                }

                continue;
            }

            $time = time();

            $this->log('Found a job <pop>'.$job.'</pop>', Logger::NOTICE);

            $this->workingOn($job);

            Event::fire(Event::WORKER_FORK, array($this, $job));

            // Fork into another process
            $this->child = pcntl_fork();

            // Returning -1 means error in forking
            if ($this->child == -1) {
                Event::fire(Event::WORKER_FORK_ERROR, array($this, $job));

                $this->log('Unable to fork process, this is a fatal error, aborting worker', Logger::ALERT);
                $this->log('Re-queuing job <pop>'.$job.'</pop>', Logger::INFO);

                // Because it wasn't the job that failed the job is readded to the queue
                // so that in can be tried again at a later time
                $job->queue();

                $this->shutdown();
            } elseif ($this->child > 0) {
                // In parent if $pid > 0 since pcntl_fork returns process id of child
                Event::fire(Event::WORKER_FORK_PARENT, array($this, $job, $this->child));

                $this->log('Forked process to run job on pid:'.$this->child, Logger::DEBUG);
                $this->updateProcLine('Worker: forked '.$this->child.' at '.strftime('%F %T'));

                // Set the PID in redis
                $this->redis->hset(self::redisKey($this), 'job_pid', $this->child);

                // Wait until the child process finishes before continuing
                $i = 0;
                // pcntl_wait($status);
                $this->handleRemoteSignal();
                $pid = $this->child;

                if($pid > 0) {
                    $this->redis->hset(self::redisKey($this), 'job_load', str_replace("\n", "", trim(shell_exec ( "ps -p " . $pid . " -o %cpu,%mem | tail -n +2" ))));
                }
                $t = microtime(true);
                while(pcntl_wait($status, WNOHANG) === 0) {
                    usleep(5000);
                    $elapsed = microtime(true) - $t;
                    if( ($elapsed) > 5) {
                        $t = microtime(true);
                        if((time() - $time) > 3600) {
                            $this->log('Expired job, running too long (>1hr)', Logger::DEBUG);
                            break;
                        }
                        if($pid > 0) {
                            $this->redis->hset(self::redisKey($this), 'job_load', str_replace("\n", "", trim(shell_exec ( "ps -p " . $pid . " -o %cpu,%mem | tail -n +2" ))));
                        }

                        $this->host->working($this);
                        $this->log('(' . $status . ') [' . $this->getId() . '] Host keep alive and child (' . $this->child . ') still up (' . (time() - $time) . 's)', Logger::DEBUG);
                    }
                }

                if (!pcntl_wifexited($status) or ($exitStatus = pcntl_wexitstatus($status)) !== 0) {
                    if ($this->job->getStatus() == Job::STATUS_FAILED) {
                        $this->log('Job '.$job.' failed: "'.$job->failError().'" in '.$this->job->execTimeStr(), Logger::ERROR);
                        $this->shutdown();
                    } else {
                        if(empty($exitStatus)) {
                            $exitStatus = 255;
                        }
                        $this->log('Job '.$job.' exited with code '.$exitStatus, Logger::ERROR);
                        $this->job->fail(new Exception\Dirty('Job '.$job.' exited with code '.$exitStatus));
                        $this->shutdown();
                    }

                } else {
                    if (($this->job->toArray()['worker'] === $this->getId()) && $this->job->getStatus() == Job::STATUS_RUNNING) {
                        $this->log('Job '.$job.' abnormally exited with code '.$exitStatus, Logger::ERROR);
                        $this->job->fail(new Exception\Dirty('Job '.$job.' abnormally exited with code '.$exitStatus));
                        $this->shutdown();
                    }
                }
            } else {
                // Reset the redis connection to prevent forking issues
                $this->redis->disconnect();
                $this->redis->connect();
                $this->redis->client("setname", "job:" . $job->getId());

                $this->redis->hset(self::redisKey($this), 'job_desc', (string) $job);

                Event::fire(Event::WORKER_FORK_CHILD, array($this, $job, getmypid()));

                $this->log('Running job <pop>'.$job.'</pop>', Logger::INFO);
                $this->updateProcLine('Job: processing '.$job->getQueue().'#'.$job->getId().' since '.strftime('%F %T'));
                $packetData = $job->getPacket();
                if($packetData['payload']) {
                    $payload = json_decode($packetData['payload'], true)['data'];
                    if(!empty($payload['series_id'])) {
                        $seriesId = $payload['series_id'];
                        $this->redis->multi();
                        if(is_array($seriesId)) {
                            foreach($seriesId as $sid) {
                                $this->redis->zadd("jobseries:{$sid}", time(), $job->getId());
                                $this->redis->expire("jobseries:{$sid}", \Resque::DEFAULT_EXPIRY_TIME);
                            }
                        } else {
                            $this->redis->zadd("jobseries:{$seriesId}", time(), $job->getId());
                            $this->redis->expire("jobseries:{$seriesId}", \Resque::DEFAULT_EXPIRY_TIME);
                        }
                        $this->redis->exec();
                    }
                }
                $job->setLogger($this->getLogger());
                $this->perform($job);
                exit(0);
            }

            $this->child = null;
            $this->doneWorking();
        }
    }

    /**
     * Process a single job
     *
     * @param Job $job The job to be processed.
     */
    public function perform(Job $job)
    {
        // Set timeout so as to stop any hanged jobs
        // and turn off displaying errors as it fills
        // up the console
        set_time_limit($this->timeout);
        ini_set('display_errors', 0);
        ob_implicit_flush(true);

        $job->isPerformedOnBot = true;

        $job->perform();

        $status = $job->getStatus();

        switch ($status) {
            case Job::STATUS_COMPLETE:
            $this->log('Done job <pop>'.$job.'</pop> in <pop>'.$job->execTimeStr().'</pop>', Logger::INFO);
            break;
            case Job::STATUS_CANCELLED:
            $this->log('Cancelled job <pop>'.$job.'</pop>', Logger::INFO);
            break;

            case Job::STATUS_FAILED:
            $this->log('Job '.$job.' failed: "'.$job->failError().'" in '.$job->execTimeStr(), Logger::ERROR);
            break;
            case Job::STATUS_WAITING:
            $this->log('Job '.$job.' requeued explicitly.', Logger::INFO);
            break;
            case Job::STATUS_DELAYED:
            $this->log('Job '.$job.' requeued with delay.', Logger::INFO);
            break;
            default:
            $this->log('Unknown job status "('.gettype($status).')'.$status.'" for <pop>'.$job.'</pop>', Logger::WARNING);
            break;
        }

        return $status;
    }

    /**
     * Perform necessary actions to start a worker
     */
    protected function startup()
    {
        $this->host->cleanup();
        $this->cleanup();
        $this->register();

        $cleaned = Job::cleanup($this->queues);
        if ($cleaned['zombie']) {
            $this->log('Failed <pop>'.$cleaned['zombie'].'</pop> zombie job'.($cleaned['zombie'] == 1 ? '' : 's'), Logger::NOTICE);
        }
        if ($cleaned['processed']) {
            $this->log('Cleared <pop>'.$cleaned['processed'].'</pop> processed job'.($cleaned['processed'] == 1 ? '' : 's'), Logger::NOTICE);
        }

        $this->cleanupQueue();

        $this->setStatus(self::STATUS_RUNNING);

        Event::fire(Event::WORKER_STARTUP, $this);
    }

    /**
     * Schedule a worker for shutdown. Will finish processing the current job
     * and when the timeout interval is reached, the worker will shut down.
     */
    public function shutdown()
    {
        $this->shutdown = true;
        $this->redis->hmset(self::redisKey($this), 'shutdown', true);

        Event::fire(Event::WORKER_SHUTDOWN, $this);
    }

    /**
     * Force an immediate shutdown of the worker, killing any child jobs
     * currently running.
     */
    public function forceShutdown()
    {
        Event::fire(Event::WORKER_FORCE_SHUTDOWN, $this);

        if ($this->child === 0) {
            $this->log('Forcing shutdown of job <pop>'.$this->job.'</pop>', Logger::NOTICE);
        } else {
            $this->log('Forcing shutdown of worker <pop>'.$this.'</pop>', Logger::NOTICE);
        }

        $this->shutdown();
        $this->killChild(SIGKILL);
    }

    /**
     * Cancel the currently running job
     */
    public function cancelJob()
    {
        try {
            $this->killChild();
        } catch (Exception\Shutdown $e) {
            throw new Exception\Cancel('Cancel signal received');
        }
    }

    /**
     * Kill a forked child job immediately. The job it is processing will not
     * be completed.
     */
    public function killChild($signal = SIGUSR1)
    {
        if (is_null($this->child)) {
            return;
        }

        if ($this->child === 0) {
            $this->log('Children is cancelling', Logger::NOTICE);
            $this->job->cancel();
            throw new Exception\Shutdown('Job forced shutdown');
        }

        Event::fire(Event::WORKER_KILLCHILD, array($this, $this->child));
        $this->log('Killing child process at pid:'.$this->child, Logger::DEBUG);
        posix_kill($this->child, $signal);

        $this->child = null;
    }

    /**
     * Register this worker in Redis and signal handlers that a worker should respond to
     *
     * - TERM: Shutdown immediately and stop processing jobs
     * - INT: Shutdown immediately and stop processing jobs
     * - QUIT: Shutdown after the current job finishes processing
     * - USR1: Kill the forked child immediately and continue processing jobs
     */
    public function register()
    {
        $this->log('Registering worker <pop>'.$this.'</pop>', Logger::NOTICE);

        $this->host->working($this);
        $this->redis->client("setname", "worker:" . $this->id);
        $this->redis->sadd(self::redisKey(), $this->id);

        $previouslyRegistered = $this->redis->hexists(self::redisKey($this), 'started');

        $reg = array(
            'started'      => microtime(true),
            'hostname'     => (string)$this->host,
            'pid'          => getmypid(),
            'memory'       => memory_get_usage(),
            'memory_limit' => $this->memoryLimit,
            'queues'       => implode(',', $this->queues),
            'shutdown'     => false,
            'blocking'     => $this->blocking,
            'status'       => $this->status,
            'interval'     => $this->interval,
            'timeout'      => $this->timeout,
            'processed'    => 0,
            'cancelled'    => 0,
            'failed'       => 0,
            'job_id'       => '',
            'job_pid'      => 0,
            'job_started'  => 0
        );

        if($previouslyRegistered) {
            $reg['restarted'] = microtime(true);
            if(empty($reg['time_slice'])) $reg['time_slice'] = 0;
            if(empty($reg['count'])) $reg['count'] = 0;
            if(empty($reg['time_slice_across_restart'])) $reg['time_slice_across_restart'] = 0;
            if(empty($reg['count_across_restart'])) $reg['count_across_restart'] = 0;
            $reg['time_slice_across_restart'] += $reg['time_slice'];
            $reg['time_slice'] = 0;
            $reg['count_across_restart'] += $reg['count'];
            $reg['count'] = 0;
            unset($reg['started']);
        }

        $this->redis->hmset(self::redisKey($this), $reg);

        if (function_exists('pcntl_signal')) {
            $this->log('Registering sig handlers for worker '.$this, Logger::DEBUG);

            // PHP 7.1 allows async signals
            if (function_exists('pcntl_async_signals')) {
                pcntl_async_signals(true);
            } else {
                declare(ticks = 1);
            }
            pcntl_signal(SIGTERM, array($this, 'sigForceShutdown'));
            pcntl_signal(SIGINT, array($this, 'sigForceShutdown'));
            pcntl_signal(SIGQUIT, array($this, 'sigShutdown'));
            pcntl_signal(SIGUSR1, array($this, 'sigCancelJob'));
            pcntl_signal(SIGUSR2, array($this, 'sigPause'));
            pcntl_signal(SIGCONT, array($this, 'sigResume'));
            pcntl_signal(SIGPIPE, array($this, 'sigWakeUp'));
        }

        register_shutdown_function(array($this, 'unregister'));

        Event::fire(Event::WORKER_REGISTER, $this);
    }

    /**
     * Unregister this worker in Redis
     */
    public function unregister($force = false)
    {
        if(!$force) {
            if ($this->child === 0) {
            // This is a child process so don't unregister worker
            // However if the shutdown was due to an error, for instance the job hitting the
            // max execution time, then catch the error and fail the job
                if (($error = error_get_last()) and in_array($error['type'], $this->shutdownErrors)) {
                    $extendedOutput = "Exception";
                    $this->job->fail(new \ErrorException($error['message'], $error['type'], 0, $error['file'], $error['line']), "");
                }

                return;
            } elseif (!is_null($this->child)) {
            // There is a child process running
                $this->log('There is a child process pid:'.$this->child.' running, killing it', Logger::INFO);
                $this->killChild();
            }

            if (is_object($this->job)) {
                $this->job->fail(new Exception\Dirty);
                $this->log('Failing running job <pop>'.$this->job.'</pop>', Logger::NOTICE);
            }
        }

        $this->log('Unregistering worker <pop>'.$this.'</pop>', Logger::NOTICE);

        $this->redis->srem(self::redisKey(), $this->id);
        $this->cleanupQueue();
        $this->redis->expire(self::redisKey($this), \Resque::getConfig('default.expiry_time', \Resque::DEFAULT_EXPIRY_TIME));

        $this->host->finished($this);

        // Remove pid file if one set
        if (!is_null($this->pidFile) and getmypid() === (int)trim(file_get_contents($this->pidFile))) {
            unlink($this->pidFile);
        }

        Event::fire(Event::WORKER_UNREGISTER, $this);
    }

    /**
     * Signal handler callback for TERM or INT, forces shutdown of worker
     *
     * @param int $sig Signal that was sent
     */
    public function sigForceShutdown($sig)
    {
        switch ($sig) {
            case SIGTERM:
            $sig = 'TERM';
            break;
            case SIGINT:
            $sig = 'INT';
            break;
            default:
            $sig = 'Unknown';
            break;
        }

        $this->log($sig.' received; force shutdown worker', Logger::INFO);
        $this->forceShutdown();
    }

    /**
     * Signal handler callback for QUIT, shutdown the worker.
     */
    public function sigShutdown()
    {
        $this->log('QUIT received; shutdown worker', Logger::INFO);
        $this->shutdown();
    }

    /**
     * Signal handler callback for USR1, cancel current job.
     */
    public function sigCancelJob()
    {
        $this->log('USR1 received; cancel current job', Logger::INFO);
        $this->cancelJob();
    }

    /**
     * Signal handler callback for USR2, pauses processing of new jobs.
     */
    public function sigPause()
    {
        $this->log('USR2 received; pausing job processing', Logger::INFO);
        $this->setStatus(self::STATUS_PAUSED);
    }

    /**
     * Signal handler callback for CONT, resumes worker allowing it to pick
     * up new jobs.
     */
    public function sigResume()
    {
        $this->log('CONT received; resuming job processing', Logger::INFO);
        $this->setStatus(self::STATUS_RUNNING);
    }

    /**
     * Signal handler for SIGPIPE, in the event the Redis connection has gone away.
     * Attempts to reconnect to Redis, or raises an Exception.
     */
    public function sigWakeUp()
    {
        $this->log('SIGPIPE received; attempting to wake up', Logger::INFO);
        // $this->redis->establishConnection();
        if(!$this->redis->isConnected()) {
            $this->redis->connect();
        }
        Event::fire(Event::WORKER_WAKEUP, $this);
    }

    /**
     * Tell Redis which job we're currently working on.
     *
     * @param Job $job Job instance containing the job we're working on.
     */
    public function workingOn(Job &$job)
    {
        $this->job = $job;
        $job->setWorker($this);

        Event::fire(Event::WORKER_WORKING_ON, array($this, $job));

        $this->redis->hmset(self::redisKey($this), array(
            'job_id'      => $job->getId(),
            'job_started' => microtime(true)
        ));
    }

    /**
     * Notify Redis that we've finished working on a job, clearing the working
     * state and incrementing the job stats.
     */
    public function doneWorking()
    {
        Event::fire(Event::WORKER_DONE_WORKING, array($this, $this->job));


        $this->redis->hmset(self::redisKey($this), array(
            'job_id'      => '',
            'job_desc'      => '',
            'job_load'      => '',
            'job_pid'     => 0,
            'job_started' => 0
        ));


        $this->redis->hincrbyfloat(self::redisKey($this), 'time_slice', $this->job->execTime());
        $this->redis->hincrby(self::redisKey($this), 'count', 1);

        switch ($this->job->getStatus()) {
            case Job::STATUS_COMPLETE:
            $this->redis->hincrby(self::redisKey($this), 'processed', 1);
            break;
            case Job::STATUS_CANCELLED:
            $this->redis->hincrby(self::redisKey($this), 'cancelled', 1);
            break;
            case Job::STATUS_FAILED:
            $this->redis->hincrby(self::redisKey($this), 'failed', 1);
            break;
            case Job::STATUS_WAITING:
            case Job::STATUS_DELAYED:
            $this->redis->hincrby(self::redisKey($this), 'retried', 1);
            break;
        }

        $this->job = null;
    }

    /**
     * Fetch the packet for the worker
     *
     * @return array|false The worker packet or false
     */
    public function getPacket()
    {
        if ($packet = $this->redis->hgetall(self::redisKey($this))) {
            if(!empty($packet['time_slice'])) {
                $packet['utilization'] = ($packet['time_slice'] / ( time() - $packet['started'] ) * 100);
            }
            if(!empty($packet['incapabilities'])) {
                $packet['incapabilities'] = json_decode($packet['incapabilities'], true);
                $packet['capabilities'] = json_decode($packet['capabilities'], true);
            }
            return $packet;
        }

        return false;
    }

    /**
     * Update the status indicator for the current worker with a new status.
     *
     * @param int $status The status of the worker
     */
    public function setStatus($status)
    {
        $this->redis->hset(self::redisKey($this), 'status', $status);

        $oldstatus = $this->status;
        $this->status = $status;

        switch ($status) {
            case self::STATUS_NEW:
            break;
            case self::STATUS_RUNNING:
            if ($oldstatus != self::STATUS_NEW) {
                Event::fire(Event::WORKER_RESUME, $this);
            }

            break;
            case self::STATUS_PAUSED:
            Event::fire(Event::WORKER_PAUSE, $this);
            break;
        }
    }

    /**
     * Return an array containing all of the queues that this worker should use
     * when searching for jobs.
     *
     * If * is found in the list of queues, every queue will be searched in
     * alphabetic order.
     *
     * @return array Array of associated queues.
     */
    public function resolveQueues()
    {
        if (in_array('*', $this->queues)) {
            $queues = $this->redis->smembers(Queue::redisKey());
            is_array($queues) and sort($queues);
        } else {
            $queues = $this->queues;
        }

        if (!is_array($queues)) {
            $queues = array();
        }

        return array_merge($queues, [ $this->getId() ]);
    }

    /**
     * Find any delayed jobs and add them to the queue if found
     *
     * @param int $endTime   optional end time for range
     * @param int $startTime optional start time for range
     */
    public function queueDelayed($endTime = null, $startTime = 0)
    {
        $startTime = $startTime ?: 0;
        $endTime = $endTime ?: time();

        foreach ($this->resolveQueues() as $queue) {
            $this->redis->multi();
            $jobs = $this->redis->zrangebyscore(Queue::redisKey($queue, 'delayed'), $startTime, $endTime);
            $this->redis->zremrangebyscore(Queue::redisKey($queue, 'delayed'), $startTime, $endTime);
            list($jobs, $found) = $this->redis->exec();

            if ($found > 0) {
                foreach ($jobs as $payload) {
                    $job = Job::loadPayload($queue, $payload);
                    $job->setWorker($this);

                    if (Event::fire(Event::JOB_QUEUE_DELAYED, $job) !== false) {
                        $job->queue(false);

                        Event::fire(Event::JOB_QUEUED_DELAYED, $job);
                    }
                }

                Stats::decr('delayed', $found);
                Stats::decr('delayed', $found, Queue::redisKey($queue, 'stats'));

                $this->log('Added <pop>'.$found.'</pop> delayed job'.($found == 1 ? '' : 's').' to <pop>'.$queue.'</pop> queue', Logger::NOTICE);
            }
        }
    }

    /**
     * Look for any workers which should be running on this server and if
     * they're not, remove them from Redis.
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the workers did not die gracefully
     * and therefore leave state information in Redis.
     */
    public function cleanup()
    {
        $workers = self::allWorkers();
        $hosts   = $this->redis->smembers(Host::redisKey());
        $cleaned = array();

        foreach ($workers as $worker) {
            list($host, $pid) = explode(':', (string)$worker, 2);

            if (
                ($host != (string)$this->host and in_array($host, $hosts)) or
                ($host == (string)$this->host and posix_kill((int)$pid, 0))
            ) {
                continue;
            }

            $this->log('Pruning dead worker: '.$worker, Logger::DEBUG);

            $worker->unregister(true);
            $cleaned[] = (string)$worker;
        }

        $workerIds = array_map(
            function ($w) {
                return (string)$w;
            },
            $workers
        );
        $keys = (array)$this->redis->keys('worker:'.$this->host.':*');

        foreach ($keys as $key) {
            $key = $this->redis->removeNamespace($key);
            $id = substr($key, strlen('worker:'));

            if (!in_array($id, $workerIds)) {
                if ($this->redis->ttl($key) < 0) {
                    $this->log('Expiring worker data: '.$key, Logger::DEBUG);

                    $this->redis->expire($key, \Resque::getConfig('default.expiry_time', \Resque::DEFAULT_EXPIRY_TIME));
                }
            }
        }

        Event::fire(Event::WORKER_CLEANUP, array($this, $cleaned));

        return $cleaned;
    }

    /**
     * Return all known workers
     *
     * @return array
     */
    public static function allWorkers(Logger $logger = null)
    {
        if (!($ids = Redis::instance()->smembers(self::redisKey()))) {
            return array();
        }

        $workers = array();
        foreach ($ids as $id) {
            if (($worker = self::fromId($id, $logger)) !== false) {
                $workers[] = $worker;
            }
        }

        return $workers;
    }

    /**
     * Return host worker by id
     *
     * @param  string      $id     Worker id
     * @param  string      $host   Hostname
     * @param  Logger      $logger Logger
     * @return array|false
     */
    public static function hostWorker($id, $host = null, Logger $logger = null)
    {
        $workers = self::hostWorkers($host);

        foreach ($workers as $worker) {
            if ((string)$id == (string)$worker and posix_kill($worker->getPid(), 0)) {
                return $worker;
            }
        }

        return false;
    }

    /**
     * Return all known workers
     *
     * @param  string $host   Hostname
     * @param  Logger $logger Logger
     * @return array
     */
    public static function hostWorkers($host = null, Logger $logger = null)
    {
        if (!($ids = Redis::instance()->smembers(self::redisKey()))) {
            return array();
        }

        $host = $host ?: gethostname();

        $workers = array();
        foreach ($ids as $id) {
            if (
                (strpos($id, $host.':') !== false) and
                ($worker = self::fromId($id, $logger)) !== false
            ) {
                $workers[] = $worker;
            }
        }

        return $workers;
    }

    /**
     * Get the worker id.
     *
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Set the worker id
     *
     * @param string $id Id to set to
     */
    public function setId($id)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker id after worker has started working');
        }

        $this->id = $id;
    }

    /**
     * Get the worker queues.
     *
     * @return string
     */
    public function getQueues()
    {
        return $this->queues;
    }

    /**
     * Set the worker queues
     *
     * @param  string $queues Queues for worker to watch
     * @return array
     */
    public function setQueues($queues)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker queues after worker has started working');
        }

        $this->queues = $queues;
    }

    /**
     * Get the worker process id.
     *
     * @return int
     */
    public function getPid()
    {
        return $this->pid;
    }

    /**
     * Set the worker process id - this is done when
     * worker is loaded from memory
     *
     * @param int $pid Set worker pid
     */
    public function setPid($pid)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker pid after worker has started working');
        }

        $this->pid = (int)$pid;
    }

    /**
     * Get the worker pid file.
     *
     * @return string
     */
    public function getPidFile()
    {
        return $this->pidFile;
    }

    /**
     * Set the worker pid file
     *
     * @param  string     $pidFile Filename to store pid in
     * @throws \Exception
     */
    public function setPidFile($pidFile)
    {
        $dir = realpath(dirname($pidFile));
        $filename = basename($pidFile);

        if (substr($pidFile, -1) == '/' or $filename == '.') {
            throw new \InvalidArgumentException('The pid file "'.$pidFile.'" must be a valid file path');
        }

        if (!is_dir($dir)) {
            throw new \RuntimeException('The pid file directory "'.$dir.'" does not exist');
        }

        if (!is_writeable($dir)) {
            throw new \RuntimeException('The pid file directory "'.$dir.'" is not writeable');
        }

        $this->pidFile = $dir.'/'.$filename;

        if (file_exists($this->pidFile) and posix_kill((int)trim(file_get_contents($this->pidFile)), 0)) {
            throw new \RuntimeException('Pid file "'.$pidFile.'" already exists and worker is still running.');
        }

        if (!file_put_contents($this->pidFile, getmypid(), LOCK_EX)) {
            throw new \RuntimeException('Could not write pid to file "'.$pidFile.'"');
        }
    }

    /**
     * Get the queue host.
     *
     * @return Host
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * Set the queue host
     *
     * @param Host $host The host to set for this worker
     */
    public function setHost(Host $host)
    {
        $this->host = $host;

        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker host after worker has started working');
        }
    }

    /**
     * Get the logger instance
     *
     * @return Logger
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * Set the logger instance
     *
     * @param Logger $logger The logger for this worker
     */
    public function setLogger(Logger $logger = null)
    {
        $this->logger = $logger;
    }

    /**
     * Helper function that passes through to logger instance
     *
     * @see    Logger::log For more documentation
     * @return mixed
     */
    public function log()
    {
        if ($this->logger !== null) {
            return call_user_func_array(array($this->logger, 'log'), func_get_args());
        }

        return false;
    }

    /**
     * Get the queue blocking.
     *
     * @return bool
     */
    public function getBlocking()
    {
        return $this->blocking;
    }

    /**
     * Set the queue blocking
     *
     * @param bool $blocking Should worker use Redis blocking
     */
    public function setBlocking($blocking)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker blocking after worker has started working');
        }

        $this->blocking = (bool)$blocking;
    }

    /**
     * Get the worker interval.
     *
     * @return int
     */
    public function getInterval()
    {
        return $this->interval;
    }

    /**
     * Set the worker interval
     *
     * @param int $interval The worker interval
     */
    public function setInterval($interval)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker interval after worker has started working');
        }

        $this->interval = $interval;
    }

    /**
     * Get the worker queue timeout.
     *
     * @return int Worker queue timeout
     */
    public function getTimeout()
    {
        return $this->timeout;
    }

    /**
     * Set the worker queue timeout
     *
     * @param  string $timeout Worker queue timeout
     * @return string
     */
    public function setTimeout($timeout)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker timeout after worker has started working');
        }

        $this->timeout = $timeout;
    }

    /**
     * Get the queue memory limit.
     *
     * @return int Memory limit
     */
    public function getMemoryLimit()
    {
        return $this->memoryLimit;
    }

    /**
     * Set the queue memory limit
     *
     * @param int $memoryLimit Memory limit
     */
    public function setMemoryLimit($memoryLimit)
    {
        if ($this->status != self::STATUS_NEW) {
            throw new \RuntimeException('Cannot set worker memory limit after worker has started working');
        }

        $this->memoryLimit = $memoryLimit;
    }

    /**
     * Return array representation of this job
     *
     * @return array
     */
    public function toArray()
    {
        $packet = $this->getPacket();

        return array(
            'id'           => (string)$this->id,
            'hostname'     => (string)$packet['hostname'],
            'pid'          => (int)$packet['pid'],

            'queues'       => array(
                'selected' => (array)explode(',', $packet['queues']),
                'resolved' => (array)$this->resolveQueues()
            ),
            'shutdown'     => (bool)$packet['shutdown'],
            'blocking'     => (bool)$packet['blocking'],
            'status'       => (int)$packet['status'],
            'interval'     => (int)$packet['interval'],
            'timeout'      => (int)$packet['timeout'],
            'memory'       => (int)$packet['memory'],
            'memory_limit' => (int)$packet['memory_limit'] * 1024 * 1024,

            'started'      => (float)$packet['started'],
            'processed'    => (int)$packet['processed'],
            'cancelled'    => (int)$packet['cancelled'],
            'failed'       => (int)$packet['failed'],

            'job_id'       => (string)$packet['job_id'],
            'job_pid'      => (int)$packet['job_pid'],
            'job_started'  => (float)$packet['job_started'],
        );
    }

    /**
     * On supported systems, update the name of the currently running process
     * to indicate the current state of a worker.
     *
     * supported systems are
     * - PHP Version < 5.5.0 with the PECL proctitle module installed
     * - PHP Version >= 5.5.0 using built in method
     *
     * @param string $status The updated process title.
     */
    protected function updateProcLine($status)
    {
        $status = $this->getProcessTitle($status);
        if (function_exists('cli_set_process_title')) {
            cli_set_process_title($status);
            return;
        }

        if (function_exists('setproctitle')) {
            setproctitle($status);
        }
    }

    /**
     * Creates process title string from current version and status of worker
     *
     * @param string $status
     * @return string
     */
    protected function getProcessTitle($status)
    {
        return sprintf('%s: %s', gethostname(), $status);
    }

    /**
     * Determine if the memory limit has been exceeded.
     *
     * @return bool
     */
    protected function memoryExceeded()
    {
        static $warning_percent = 0.5;

        $percent = (memory_get_usage() / 1024 / 1024) / $this->memoryLimit;

        if ($percent >= $warning_percent) {
            $this->log(sprintf('Memory usage at %d%% (Max %s MB)', round($percent * 100, 1), $this->memoryLimit), Logger::DEBUG);
            $warning_percent = ceil(($percent * 100) / 10) * 10 / 100;
        }

        return $percent > 0.999;
    }

    /**
     * Returns formatted interval string
     *
     * @return string
     */
    protected function interval_string()
    {
        return $this->interval.' second'.($this->interval == 1 ? '' : 's');
    }


    public function cleanupQueue() {

        $queues = $this->redis->smembers(Queue::redisKey());

        foreach($queues as $queue) {
            $this->log('Cleaning up zombie queue <pop>'.$queue.'</pop>', Logger::NOTICE);
            while($this->redis->rpoplpush("queue:{$queue}:{$this->id}:processing_list", $this->redis->addNamespace("queue:{$queue}"))) {
                $this->redis->hincrby("stats", "running", -1);
                $this->redis->hincrby("stats", "queued", 1);
            }
            $this->redis->del("queue:{$queue}:{$this->id}:processing_list");
        }

        // $pipeline = $this->redis->pipeline();

        while($item = $this->redis->rpoplpush("queue:{$this->id}", $this->redis->addNamespace("queue:default"))) {
            //... pushing every item in the dedicated queue back to default queue
            error_log("$item is repushed");
        }
        $this->redis->del("queue:{$this->id}");
        $this->redis->del("queue:{$this->id}:stats");
        $this->redis->del("queue:{$this->id}:processed");
        $this->redis->del("queue:{$this->id}:cancelled");
        $this->redis->del("queue:{$this->id}:failed");
        $this->redis->del("queue:{$this->id}:delayed");

        // $pipeline->execute();
        /**
         * Removal of dedicated queues stats, optional..
         */

    }


    /**
     * @return void
     * @author 
     **/
    public function setCapability($capabilities)
    {
        $this->capabilities = $capabilities;
        $this->redis->hset(self::redisKey($this), 'capabilities', json_encode($this->capabilities));
    }


    /**
     *
     * @return void
     * @author 
     **/
    public function clearCapabilities()
    {
        $this->capabilities = array();
        $this->redis->hset(self::redisKey($this), 'capabilities', json_encode($this->capabilities));
    }


    /**
     *
     * @return void
     * @author 
     **/
    public function clearIncapabilities()
    {
        $this->incapabilities = array();
        $this->redis->hset(self::redisKey($this), 'incapabilities', json_encode($this->incapabilities));
    }


    /**
     *
     * @return void
     * @author 
     **/
    public function addIncapability($item)
    {
        if(!in_array($item, $this->incapabilities)) {
            array_push($this->incapabilities, $item);
        }
        $this->redis->hset(self::redisKey($this), 'incapabilities', json_encode($this->incapabilities));
    }


    public function getIncapabilities($item)
    {
        $this->incapabilities = $this->redis->hget(self::redisKey($this), 'incapabilities');
        if(empty($this->incapabilities )) {
            $this->incapabilities  = array();
        }
        return $this->incapabilities;
    }


    public function getCapabilities()
    {
        return $this->capabilities;
    }


    public static function friendlyStatus($status) {
        switch($status) {
            case self::STATUS_NEW:
            return "NEW";

            case self::STATUS_RUNNING:
            return "RUNNING";

            case self::STATUS_PAUSED:
            return "PAUSED";

        }
    }

    public static function dumpWorker($asText = true) {

        $buffer = new \Symfony\Component\Console\Output\BufferedOutput();
        $table = new \Symfony\Component\Console\Helper\Table($buffer);

        $return = [];

        foreach(self::allWorkers() as $worker) {
            $return[] = array_merge($worker->getPacket(), ['id' => $worker->getId()]);
        }

        $table->setHeaders([
            'Worker Id (Total: ' . count($return) . ')','Queues','Status','Job PID','Job Last Started', "Capabilities"
        ])
        ->setRows(array_map(function($d) {
            return [
                $d['id'],
                $d['queues'],
                self::friendlyStatus($d['status']) . ' (' . $d['status'] . ')',
                $d['job_pid'],
                empty($d['job_started']) ? "---" : date("Y-m-d H:i:s", $d['job_started']),
                implode(" & ", $d['capabilities'])
            ];
        }, $return));

        $table->render();

        if($asText)
            return $buffer->fetch();

        return $return;
    }
}
