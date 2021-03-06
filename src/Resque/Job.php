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

use Closure;
use Resque\Helpers\Stats;

/**
 * Resque job class
 *
 * @author Michael Haynes <mike@mjphaynes.com>
 */
class Job
{

    // Job status constants
    const STATUS_WAITING   = 1;
    const STATUS_DELAYED   = 2;
    const STATUS_RUNNING   = 3;
    const STATUS_COMPLETE  = 4;
    const STATUS_CANCELLED = 5;
    const STATUS_FAILED    = 6;

    /**
     * How many times should normal exception be retried
     */
    const RETRY_THRESHOLD = 3;

    /**
     * Job ID length
     */
    const ID_LENGTH = 22;

    /**
     * @var Redis The Redis instance
     */
    protected $redis;

    /**
     * @var string The name of the queue that this job belongs to
     */
    protected $queue;

    /**
     * @var array The payload sent through for this job
     */
    protected $payload;

    /**
     * @var string The ID of this job
     */
    protected $id;

    /**
     * @var string The classname this job
     */
    protected $class;

    /**
     * @var string The method name for this job
     */
    protected $method = 'perform';

    /**
     * @var string The data/arguments for the job
     */
    protected $data;

    /**
     * @var Worker Instance of the worker running this job
     */
    protected $worker;

    /**
     * @var object Instance of the class performing work for this job
     */
    protected $instance;

    /**
     * @var array subjects to mark state onto
     */
    protected $subjects = [];

    protected $logger = null;

    /**
     * @var array of statuses that are considered final/complete
     */
    protected static $completeStatuses = array(
        self::STATUS_FAILED,
        self::STATUS_COMPLETE,
        self::STATUS_CANCELLED
    );

    public $isPerformedOnBot = false;

    /**
     * Get the Redis key
     *
     * @param  Job    $job    the job to get the key for
     * @param  string $suffix to be appended to key
     * @return string
     */
    public static function redisKey($job, $suffix = null)
    {
        $id = $job instanceof Job ? $job->id : $job;
        return 'job:'.$id.($suffix ? ':'.$suffix : '');
    }

    /**
     * Create a new job and save it to the specified queue.
     *
     * @param  string $queue  The name of the queue to place the job in
     * @param  string $class  The name of the class that contains the code to execute the job
     * @param  array  $data   Any optional arguments that should be passed when the job is executed
     * @param  int    $run_at Unix timestamp of when to run the job to delay execution
     * @return string
     */
    public static function create($queue, $class, array $data = null, $run_at = 0)
    {
        $id = static::createId($queue, $class, $data, $run_at);

        $job = new static($queue, $id, $class, $data);

        if ($run_at > 0) {
            if (!$job->delay($run_at)) {
                return false;
            }
        } elseif (!$job->queue()) {
            return false;
        }

        Stats::incr('total', 1);
        Stats::incr('total', 1, Queue::redisKey($queue, 'stats'));

        return $job;
    }

    /**
     * Create a new job id
     *
     * @param  string $queue  The name of the queue to place the job in
     * @param  string $class  The name of the class that contains the code to execute the job
     * @param  array  $data   Any optional arguments that should be passed when the job is executed
     * @param  int    $run_at Unix timestamp of when to run the job to delay execution
     * @return string
     */
    public static function createId($queue, $class, $data = null, $run_at = 0)
    {
        $id = dechex(crc32($queue)).
        dechex(microtime(true) * 1000).
        md5(json_encode($class).json_encode($data).$run_at.uniqid('', true));

        return substr($id, 0, self::ID_LENGTH);
    }

    /**
     * Load a job from id
     *
     * @param  string $id The job id
     * @return string
     */
    public static function load($id)
    {
        $packet = Redis::instance()->hgetall(self::redisKey($id));

        if (empty($packet) or empty($packet['queue']) or !count($payload = json_decode($packet['payload'], true))) {
            return null;
        }

        return new static($packet['queue'], $payload['id'], $payload['class'], $payload['data']);
    }

    /**
     * Load a job from the Redis payload
     *
     * @param  string $queue   The name of the queue to place the job in
     * @param  string $payload The payload that was stored in Redis
     * @return string
     */
    public static function loadPayload($queue, $payload)
    {
        $payload = json_decode($payload, true);

        if (!is_array($payload) or !count($payload)) {
            throw new \InvalidArgumentException('Supplied $payload must be a json encoded array.');
        }

        return new static($queue, $payload['id'], $payload['class'], $payload['data']);
    }

    /**
     * Create a new job
     *
     * @param string $queue Queue to add job to
     * @param string $id    Job id
     * @param string $class Job class to run
     * @param array  $data  Any Job data
     */
    public function __construct($queue, $id, $class, array $data = null)
    {
        $this->redis = Redis::instance();

        if (!is_string($queue) or empty($queue)) {
            throw new \InvalidArgumentException('The Job queue "('.gettype($queue).')'.$queue.'" must a non-empty string');
        }

        $this->queue = $queue;
        $this->id    = $id;
        $this->data  = $data;

        if ($class instanceof Closure) {
            $this->class = 'Resque\Helpers\ClosureJob';
            $this->data  = $class;
        } else {
            $this->class = $class;
            if (strpos($this->class, '@')) {
                list($this->class, $this->method) = explode('@', $this->class, 2);
            }

            // Remove any spaces or back slashes
            $this->class = trim($this->class, '\\ ');
        }

        $this->payload = $this->createPayload();

        Event::fire(Event::JOB_INSTANCE, $this);
    }

    public function getGenericPresentation() {

        return sprintf(
            '%s(%s)',
            $this->class,
            empty($this->data) ? '' : json_encode($this->data)
        );
    }

    /**
     * Generate a string representation of this object
     *
     * @return string Representation of the current job status class
     */
    public function __toString()
    {
        return sprintf(
            '%s:%s#%s(%s)',
            $this->queue,
            $this->class,
            $this->id,
            empty($this->data) ? '' : json_encode($this->data)
        );
    }

    /**
     * Save the job to Redis queue
     *
     * @return bool success
     */
    public function queue($validate = true, $queueEvenCompleted = false)
    {
        $currentStatus = $this->getStatus();

        if($currentStatus == self::STATUS_WAITING) {
            return false;
        }
        if($currentStatus == self::STATUS_RUNNING) {
            return false;
        }
        if($currentStatus == self::STATUS_COMPLETE) {
            if(!$queueEvenCompleted) return false;
        }

        if (Event::fire(Event::JOB_QUEUE, $this) === false) {
            return false;
        }

        if($validate && !$this->ensureUniqueness(false)) return false;

        if($currentStatus != self::STATUS_DELAYED) {
            $this->redis->hdel(self::redisKey($this), ['override_status']);
        }

        if(stripos($this->queue, "@") === false)
            $this->redis->sadd(Queue::redisKey(), $this->queue);

        $status = $this->redis->lpush(Queue::redisKey($this->queue), $this->payload);

        if ($status < 1) {
            return false;
        }

        $this->setStatus(self::STATUS_WAITING);

        if($currentStatus == self::STATUS_CANCELLED) {
            $this->redis->zrem(Queue::redisKey($this->queue, 'cancelled'), $this->payload);
        } else if ($currentStatus == self::STATUS_FAILED) {
            $this->redis->zrem(Queue::redisKey($this->queue, 'failed'), $this->payload);
        } else if ($currentStatus == self::STATUS_COMPLETE) {
            $this->redis->zrem(Queue::redisKey($this->queue, 'processed'), $this->payload);
        }

        Stats::incr('queued', 1);
        Stats::incr('queued', 1, Queue::redisKey($this->queue, 'stats'));
        Event::fire(Event::JOB_QUEUED, $this);

        return true;
    }

    public function ensureUniqueness($log = true) {

        if(method_exists($this->class, 'signature')) {
            $instance = $this->getInstance();
            $unique = $instance->signature($this->getData());
            $unique = "unique:job:" . $unique;
            if($log) $this->streamLog("This job requires mutex signature: " . $unique);
            if($this->redis->set($unique, $this->getId(), "NX", "EX", 7200) === 1) {
                if($log) $this->streamLog("Good. This is the first time");
                // great, this is the only job
            } else {
                // some same tag exist, check if the job is completed, if so rewrite it,
                // otherwise do not queue.
                $lastId = $this->redis->get($unique);
                $job = \Resque::job($lastId);
                if(!$job) {
                    $this->redis->set($unique, $this->getId(), "EX", 7200);
                    return true;
                }
                if($log) $this->streamLog("Existing job found (${lastId}), me: " . $this->getId());
                if($lastId == $this->getId()) {
                    $this->streamLog("OK. This is the same job, allow continue");
                    return true;
                }
                $jobStatus = $job->getStatus();
                if($log) $this->streamLog("Verifying the job status (" . $jobStatus . ")");
                if($job &&  !in_array($jobStatus, self::$completeStatuses)) {
                    if($log) $this->streamLog("NO-GO, Existing job found, but status is incomplete (" . $jobStatus . ")", true);
                    $this->redis->lpush("duplicates", $this->payload);
                    $this->redis->ltrim("duplicates", 0, 299);
                    return false;
                } else {
                    if($log) $this->streamLog("OK, Existing job found, but status is complete (" . $jobStatus . ")");
                    $this->redis->set($unique, $this->getId(), "EX", 7200);
                }
            }
        }
        return true;
    }   

    /**
     * Save the job to Redis delayed queue
     *
     * @param  int  $time unix time of when to perform job
     * @return bool success
     */
    public function delay($time)
    {
        if (Event::fire(Event::JOB_DELAY, array($this, $time)) === false) {
            return false;
        }

        if(!$this->ensureUniqueness(false)) return false;

        if(method_exists($this->class, 'onQueue')) {
            $instance = $this->getInstance();
            if(!$instance->onQueue($this)) {
                return false;
            }
        }


        $this->redis->sadd(Queue::redisKey(), $this->queue);
        $status = $this->redis->zadd(Queue::redisKey($this->queue, 'delayed'), $time, $this->payload);

        if ($status < 1) {
            return false;
        }

        $this->setStatus(self::STATUS_DELAYED);
        $this->redis->hset(self::redisKey($this), 'delayed', $time);

        Stats::incr('delayed', 1);
        Stats::incr('delayed', 1, Queue::redisKey($this->queue, 'stats'));

        Event::fire(Event::JOB_DELAYED, array($this, $time));

        return true;
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
     * Stream logs
     *
     * @return void
     * @author 
     **/
    public function streamLog($message, $setExpire = false, $del = false)
    {
        if(empty($message)) return;
        if($del) {
            $this->redis->executeRaw(["del", $this->redis->addNamespace(self::redisKey($this, 'output'))]);
            return;
        }
        $this->redis->executeRaw(["xadd", $this->redis->addNamespace(self::redisKey($this, 'output')), 'maxlen', '~', 1000, '*', 'message', $this->getWorker() . ": " . $message]);
        if($setExpire) $this->redis->executeRaw(["expire", $this->redis->addNamespace(self::redisKey($this, 'output')), 86400]);
        $this->log($this->getWorker() . ": " . $message);
    }

    /**
     * Default job fail handler
     *
     * @return void
     * @author 
     **/
    public function jobErrorHandler($errno, $errstr, $errfile = NULL, $errline = NULL, $errcontent = NULL)   
    {
        $this->streamLog("* (" . $this->friendlyErrorType($errno) . ") File: " . $errfile . ' Line:' . $errline);
        $this->streamLog("* " . $errstr);
        $this->worker && $this->worker->log($errstr, Logger::CRITICAL);
        return false;
    }

    /**
     * Default job fail handler
     *
     * @return void
     * @author 
     **/
    public function jobFatalErrorHandler()   
    {
        $last_error = error_get_last();
        if(!empty($last_error)) {
            if($last_error['type'] === E_ERROR) {
                $this->streamLog("*** FATAL ERROR ***");
                $this->jobErrorHandler($last_error['type'], $last_error['message'], $last_error['file'], $last_error['line']);
            }
        } else {
            $this->streamLog("*** GRACEFUL SHUTDOWN ***");  
        }
    }

    public function friendlyErrorType($type)
    {
        switch($type)
        {
            case E_ERROR: // 1 //
                return 'E_ERROR';
            case E_WARNING: // 2 //
                return 'E_WARNING';
            case E_PARSE: // 4 //
                return 'E_PARSE';
            case E_NOTICE: // 8 //
                return 'E_NOTICE';
            case E_CORE_ERROR: // 16 //
                return 'E_CORE_ERROR';
            case E_CORE_WARNING: // 32 //
                return 'E_CORE_WARNING';
            case E_COMPILE_ERROR: // 64 //
                return 'E_COMPILE_ERROR';
            case E_COMPILE_WARNING: // 128 //
                return 'E_COMPILE_WARNING';
            case E_USER_ERROR: // 256 //
                return 'E_USER_ERROR';
            case E_USER_WARNING: // 512 //
                return 'E_USER_WARNING';
            case E_USER_NOTICE: // 1024 //
                return 'E_USER_NOTICE';
            case E_STRICT: // 2048 //
                return 'E_STRICT';
            case E_RECOVERABLE_ERROR: // 4096 //
                return 'E_RECOVERABLE_ERROR';
            case E_DEPRECATED: // 8192 //
                return 'E_DEPRECATED';
            case E_USER_DEPRECATED: // 16384 //
                return 'E_USER_DEPRECATED';
        }
        return "";
    }

    /**
     * Perform the job
     *
     * @return bool
     */
    public function perform()
    {
        Stats::decr('queued', 1);
        Stats::decr('queued', 1, Queue::redisKey($this->queue, 'stats'));

        set_error_handler(array($this, 'jobErrorHandler'));
        register_shutdown_function(array($this, 'jobFatalErrorHandler'));

        if(!$this->ensureUniqueness()) {
            throw new Exception\Cancel("Uniqueness cannot be enforced.", 1);
            
        }

        $this->streamLog('Job begins on ' . (string) $this->getWorker(), true);

        $packet = $this->getPacket();
        $overrideCancel = (
            isset($packet['override_status']) && 
            $packet['override_status'] == Job::STATUS_CANCELLED
        );


        if($overrideCancel) {
            $this->streamLog('Remote cancelled');
            $this->cancel(new \Exception("Cancelled due to {$packet['override_reason']}"));
            return false;
        }

        if (Event::fire(Event::JOB_PERFORM, $this) === false) {
            $this->cancel(new \Exception("Cancelled due to JOB_PERFORM hook"));
            return false;
        }

        $this->run();

        $retval = true;

        try {

            Event::fire(Event::JOB_PERFORMING, [ $this->getWorker(), $this ]);

            $instance = $this->getInstance();

            $channel = null;
            if(method_exists($instance, "getChannel")) {
                $channel = $instance->getChannel();
            }

            \Core\Foundation\Log\Logger::attach(function($buffer) use ($channel) {
                $this->redis->executeRaw(["xadd", "bot-output", 'maxlen', '~', 50000, '*', 
                    'worker', $this->getWorker()->getId(),
                    'message', $buffer]);
                $this->streamLog("\n" . $buffer);
                if($channel) $this->redis->publish("bot-channel-" . $channel, $buffer);
            });

            ob_start(function($buffer, $phase) use ($channel) {
                $this->streamLog("\n" . $buffer);
                if($channel) $this->redis->publish("bot-channel-" . $channel, $buffer);
                return $buffer;
            }, 1024);

            $this->streamLog("Job started");

            if (method_exists($instance, 'setUp')) {
                $instance->setUp($this->data);
            }

            call_user_func_array(array($instance, $this->method), array($this->data, $this));

            flush();
            ob_flush();

            if (method_exists($instance, 'tearDown')) {
                $instance->tearDown();
            }

            $this->complete();

            $this->cleanupSubject();

            $this->streamLog("Job completed");


        } catch (Exception\Cancel $e) {
            // setUp said don't perform this job
            $log = "[Job cancel] [{$this->getId()}] " . $e->getMessage();
            $this->streamLog($log);
            $this->log($log, Logger::CRITICAL);
            $this->cancel($e);
            echo $e->getMessage();
            $retval = false;
        } catch (Exception\Retry $e) {
            // retry this job again
            $log = "[Job retry] [{$this->getId()}] " . $e->getMessage();
            $this->streamLog($log);
            $this->log($log, Logger::CRITICAL);
            if(!empty($e->queue)) {
                $this->queue = $e->queue;
            }
            $this->fail($e, $e->getMessage(), true);
            $retval = false;
        } catch (\Throwable $e) {
            $log = "[Job exception] [{$this->getId()}] " . $e->getMessage();
            $log .= $e->getTraceAsString();
            $this->streamLog($log);
            $this->log($log, Logger::CRITICAL);
            $this->fail($e);
            $retval = false;
        }



        $output = ob_get_contents();

        while (ob_get_length()) {
            ob_end_clean();
        }
        if(!empty($instance)) {
            if (method_exists($instance, 'output')) {
                $output .= $instance->output();
            }
        }

        $this->redis->hset(self::redisKey($this), 'output', $output);

        Event::fire(Event::JOB_DONE, $this);

        return $retval;
    }

    /**
     * Get the instantiated object for this job that will be performing work
     *
     * @return object Instance of the object that this job belongs to
     */
    public function getInstance()
    {
        if (!is_null($this->instance)) {
            return $this->instance;
        }

        if (!class_exists($this->class)) {
            throw new \RuntimeException('Could not find job class "'.$this->class.'"');
        }

        if (!method_exists($this->class, $this->method) or !is_callable(array($this->class, $this->method))) {
            throw new \RuntimeException('Job class "'.$this->class.'" does not contain a public "'.$this->method.'" method');
        }

        $class = new \ReflectionClass($this->class);

        if ($class->isAbstract()) {
            throw new \RuntimeException('Job class "'.$this->class.'" cannot be an abstract class');
        }

        $instance = $class->newInstance();

        return $this->instance = $instance;
    }

    public function getProgress()
    {
        $return = $this->redis->hmget(self::redisKey($this), [
            'status',
            'progress',
            'latest_line',
            'id',
            'exception',
            'updated',
        ]);
        $map = array(
            'status' => $return[0],
            'progress' => $return[1],
            'latest_line' => $return[2],
            'id' => $return[3],
            'exception' => $return[4],
            'updated' => $return[5],
        );
        if(!empty($map['exception'])) {
            $re = '/(.*) in (.*) on (.*)/m';
            $str = json_decode($map['exception'], true)['error'];
            $str = explode("\n", $str);
            $str = $str[0];
            if(preg_match_all($re, $str, $matches, PREG_SET_ORDER, 0)) {
                $map['exception'] = $matches[0][1];
            }
            $re = '/(.*) in (.*)/m';
            if(preg_match_all($re, $str, $matches, PREG_SET_ORDER, 0)) {
                $map['exception'] = $matches[0][1];
            }
        }
        $map['updated'] = date("H:i:s", $map['updated']);
        $map['progress'] = intval($map['progress']);
        $map['status'] = intval($map['status']);
        if($map['status'] === self::STATUS_COMPLETE) {
            $map['progress'] = 100;
        }
        return $map;
    }

    public function cleanupSubject()
    {
        if(empty($this->subjects)) {
            return false;
        }
        $jobId = $this->getId();
        foreach($this->subjects as $sub) {
            $this->redis->zrem("jobsubject:pending:${sub}", $jobId);
            $this->redis->zadd("jobsubject:done:${sub}", time(), $jobId);
            $this->redis->zremrangebyrank("svq:jobsubject:done:${sub}", 0, -10);
            $this->redis->expire("jobsubject:done:${sub}", 86400);
        }

        return true;
    }


    public function setSubject($subjects)
    {
        if(is_string($subjects)) {
            $subjects = [ $subjects ];
        }

        $subjects = array_filter($subjects);

        $this->subjects = array_merge($this->subjects, $subjects);

        $jobId = $this->getId();

        foreach($subjects as $sub) {
            $this->redis->zadd("jobsubject:pending:${sub}", time(), $jobId);
            $this->redis->expire("jobsubject:pending:${sub}", 86400 * 7);
        }

        return true;
    }

    /**
     * Report the current progress to the redis job hash map
     *
     * @return boolean
     * @author 
     **/
    public function setProgress($percent, $latestLine = "")
    {
        return $this->reportProgress($percent, $latestLine);
    }
    public function reportProgress($percent, $latestLine = "")
    {
        return $this->redis->hmset(self::redisKey($this), [
            'progress' => $percent,
            'latest_line' => $latestLine,
        ]);
    }

    /**
     * Mark the current job running
     */
    public function run()
    {
        $this->setStatus(Job::STATUS_RUNNING);

        $this->redis->zadd(Queue::redisKey($this->queue, 'running'), time(), $this->payload);
        Stats::incr('running', 1);
        Stats::incr('running', 1, Queue::redisKey($this->queue, 'stats'));

        Event::fire(Event::JOB_RUNNING, $this);
    }

    /**
     * Mark the current job stopped
     * This is an internal function as the job is either completed, cancelled or failed
     */
    protected function stopped()
    {
        $this->redis->zrem(Queue::redisKey($this->queue, 'running'), $this->payload);

        Stats::decr('running', 1);
        Stats::decr('running', 1, Queue::redisKey($this->queue, 'stats'));
    }

    /**
     * Mark the current job as complete
     */
    public function complete()
    {
        $this->stopped();

        $this->setStatus(Job::STATUS_COMPLETE);

        $this->redis->zadd(Queue::redisKey($this->queue, 'processed'), time(), $this->payload);
        $this->redis->lrem(Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 1, $this->payload);
        Stats::incr('processed', 1);
        Stats::incr('processed', 1, Queue::redisKey($this->queue, 'stats'));

        Event::fire(Event::JOB_COMPLETE, $this);
    }

    public function setSeries($series)
    {
        $this->redis->hmset(self::redisKey($this), [
            'series_id' => $series,
        ]);
    }

    public function clearSignal()
    {
        $this->redis->hdel(self::redisKey($this), [
            'override_status',
            'override_reason',
        ]);
    }

    public function signalCancel($reason = "N/A")
    {
        $this->redis->hmset(self::redisKey($this), [
            'override_status' => Job::STATUS_CANCELLED,
            'override_reason' => $reason,
        ]);
    }

    /**
     * Mark the current job as cancelled
     */
    public function cancel(\Exception $e = null)
    {
        $this->stopped();

        $this->setStatus(Job::STATUS_CANCELLED, $e);

        $this->redis->zadd(Queue::redisKey($this->queue, 'cancelled'), time(), $this->payload);
        $this->redis->lrem(Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 1, $this->payload);
        
        Stats::incr('cancelled', 1);
        Stats::incr('cancelled', 1, Queue::redisKey($this->queue, 'stats'));

        Event::fire(Event::JOB_CANCELLED, $this);
    }

    /**
     * Mark the current job as having failed
     *
     * @param \Exception $e
     */
    public function fail(\Throwable $e, $output = null, $mustRequeue = false)
    {

        $this->stopped();

        if($output != null) {
           $this->redis->hset(self::redisKey($this), 'output', $output);
       }

       // For the failed jobs we store a lot more data for debugging
       $packet = $this->getPacket();

       $this->setStatus(Job::STATUS_FAILED, $e);

       $failed_payload = array_merge(json_decode($this->payload, true), array(
        'status'    => Job::STATUS_FAILED,
        'worker'    => $packet['worker'],
        'started'   => $packet['started'],
        'finished'  => $packet['finished'],
        'output'    => $packet['output'],
        'exception' => (array)json_decode($packet['exception'], true),
    ));

    if(empty($packet['failed_count'] )) {
        $packet['failed_count'] = 0;
    }

    $threshold = self::RETRY_THRESHOLD;

    $shouldRequeue = $packet['failed_count'] < $threshold;

    $decodedPayload = json_decode($this->payload, true);

    if(!empty($decodedPayload['data']['retry_threshold'])) {
        $threshold = intval($decodedPayload['data']['retry_threshold']);
        $shouldRequeue = $packet['failed_count'] < $threshold;
        if($threshold === -2) {
            $shouldRequeue = true;
            echo "This job has specified retry_threshold = -2, and will never stop retry \n";
        }
    }

    $packet['failed_count'] += 1;

    if(!$mustRequeue) {
        $this->redis->hmset(self::redisKey($this), ['failed_count' => $packet['failed_count']]);
    }

    $remoteCancelled = (isset($packet['override_status']) && $packet['override_status'] == Job::STATUS_CANCELLED);

    if(!$remoteCancelled && ($shouldRequeue || $mustRequeue)) {
        if($this->worker) {
            if($e instanceof Exception\Retry) {
                $delay = $e->getCode();
                if ($delay < 94608000) {
                    $delay += time();
                }
                $this->delay($delay);
                $this->redis->lrem(Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 1, $this->payload);
            } else if ($packet['failed_count'] < 2) {
                /**
                 * Directly pushing back to original queue
                 */
                $this->redis->rpoplpush(
                    Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 
                    $this->redis->addNamespace(Queue::redisKey($this->queue))
                );
                Stats::incr('queued', 1);
                Stats::incr('queued', 1, Queue::redisKey($this->queue, 'stats'));
                $this->redis->zadd(Queue::redisKey($this->queue, 'fail_retried'), time(), json_encode($failed_payload));
                $this->setStatus(self::STATUS_WAITING);
            } else {
                $fails = intval($packet['failed_count']);
                if($fails > 32) $fails = 32;
                $delay = pow(2, $fails);
                if($delay > 180) {
                    $delay = 180;
                }
                $delay = mt_rand($delay / 2, $delay);
                echo "Exponential backoff ... " . $delay . 's' . PHP_EOL;
                $delay += time();
                $this->delay($delay);
                $this->redis->lrem(Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 1, $this->payload);
                $this->redis->zadd(Queue::redisKey($this->queue, 'fail_retried'), time(), json_encode($failed_payload));
            }
        }
        Stats::incr('retried', 1);
        Stats::incr('retried', 1, Queue::redisKey($this->queue, 'stats'));
    } else if ($remoteCancelled) {
        $this->run();
        $this->cancel(new \Exception("Remote cancelled: " . $packet['override_reason']));
    } else {
        $this->redis->lrem(Queue::redisKey($this->queue, $this->worker->getId() . ':processing_list'), 1, $this->payload);
        $this->redis->zadd(Queue::redisKey($this->queue, 'failed'), time(), json_encode($failed_payload));
        Stats::incr('failed', 1);
        Stats::incr('failed', 1, Queue::redisKey($this->queue, 'stats'));
    }

    Event::fire(Event::JOB_FAILURE, array($this, $e));
}

    public function getPresentation() {
        try {
            $instance = $this->getInstance();
            if(method_exists($instance, 'getPresentation')) {
                return $instance->getPresentation();
            } else {
                return $this->getClass();
            }
        } catch (\Exception $e) {
            return $this->getClass();
        }
    }

    /**
     * Returns the fail error for the job
     *
     * @return mixed
     */
    public function failError()
    {
        if (
            ($packet = $this->getPacket()) and
            $packet['status'] !== Job::STATUS_FAILED and
            ($e = json_decode($packet['exception'], true))
        ) {
            return empty($e['error']) ? var_export($e, true) : $e['error'];
        }

        return 'Unknown exception';
    }

    /**
     * Create a payload string from the given job and data
     *
     * @param  string $job
     * @param  mixed  $data
     * @return string
     */
    protected function createPayload()
    {
        if ($this->data instanceof Closure) {
            $closure = serialize(new Helpers\SerializableClosure($this->data));
            $data = compact('closure');
        } else {
            $data = $this->data;
        }

        return json_encode(array('id' => $this->id, 'class' => $this->class, 'data' => $data));
    }

    /**
     * Update the status indicator for the current job with a new status
     *
     * @param int        $status The status of the job
     * @param \Exception $e      If failed status it sends through exception
     */
    public function setStatus($status, \Throwable $e = null)
    {
        if (!($packet = $this->getPacket())) {
            $shifts = debug_backtrace();
            while($item = array_shift($shifts)) {
                if(empty($item['file'])) {
                    $item['file'] = '';
                }
                if(stripos($item['file'], 'Foundation/Retry') !== false && $item['function'] === '_exec') break;
            }
            array_splice($shifts, 3);
            $packet = array(
                'id'        => $this->id,
                'queue'     => $this->queue,
                'payload'   => $this->payload,
                'worker'    => '',
                'status'    => $status,
                'created_by'   => json_encode(array_map(function($item) {
                    if(empty($item['line'])) $item['line'] = 'na';
                    if(empty($item['file'])) $item['file'] = 'na';
                    if(empty($item['function'])) $item['function'] = 'na';
                    if(empty($item['args'])) $item['args'] = [];
                    return array(
                        'origin' => str_replace($GLOBALS['system_root'], "", $item['file']) . ':' . @$item['line'] . ' ' . @$item['function'],
                        'args' => $item['args'],
                    );
                }, $shifts)),
                'created'   => microtime(true),
                'updated'   => microtime(true),
                'delayed'   => 0,
                'started'   => 0,
                'finished'  => 0,
                'output'    => '',
                'exception' => null,
            );
        }

        $packet['worker']  = (string)$this->worker;
        $packet['status']  = $status;
        $packet['updated'] = microtime(true);

        if ($status == Job::STATUS_RUNNING) {
            $packet['started'] = microtime(true);
        }

        if (in_array($status, self::$completeStatuses)) {
            $packet['finished'] = microtime(true);
            if($status == Job::STATUS_COMPLETE) $packet['progress'] = 100;
        }

        if ($e && (!$e instanceof \Core\Job\RetryException)) {
            if($packet['exception']) {
                $exceptionPacket = json_decode($packet['exception'], true);
            } else {
                $exceptionPacket = [];
            }

            if(count($exceptionPacket) > 5) {
                array_splice($exceptionPacket, 0, count($exceptionPacket) - 5);
            } 

            if(empty($packet['failed_count'])) {
                $packet['failed_count'] = 0;
            }
            $exceptionPacket[] = array(
                'attempt' => $packet['failed_count'] + 1,
                'class'     => get_class($e),
                'error'     => sprintf('%s in %s on line %d', $e->getMessage(), $e->getFile(), $e->getLine()),
                'backtrace' => explode("\n", $e->getTraceAsString())
            );
            $packet['exception'] = json_encode($exceptionPacket);
        }

        $this->redis->hmset(self::redisKey($this), $packet);
        // $this->redis->hincrby(self::redisKey($this), ['exec_time' => $this->execTime()]);

        // Expire the status for completed jobs
        if (in_array($status, self::$completeStatuses)) {
            $expiryTime = \Resque::getConfig('default.expiry_time', \Resque::DEFAULT_EXPIRY_TIME);

            $this->redis->expire(self::redisKey($this), $expiryTime);

            if($this->isPerformedOnBot) {
                $lastRun = intval($this->redis->hget('jobs:stat:' . $this->getPresentation(), 'recent'));
                $frequency = intval($this->redis->hget('jobs:stat:' . $this->getPresentation(), 'frequency'));
                $this->redis->zincrby('jobs:time', $this->execTime(), $status . "::" . $this->getPresentation());
                $this->redis->zincrby('jobs:count', 1, $status . "::" . $this->getPresentation());

                $stat = [
                    'class_name' => $this->getClass(),
                    'recent' => time(),
                    'frequency' => ($lastRun > 0) ? ((time() - $lastRun + ( $frequency > 1 ? $frequency : 1)) / 2) : 1,

                ];

                $this->redis->hmset('jobs:stat:' . $this->getPresentation(), $stat);
            }

        }
    }


    /**
     * Fetch the packet for the job being monitored.
     *
     * @return array
     */
    public function getPacketFields($fields)
    {
        if ($packet = $this->redis->hmget(self::redisKey($this), $fields)) {
            return $packet;
        }

        return false;
    }

    /**
     * Fetch the packet for the job being monitored.
     *
     * @return array
     */
    public function getPacket()
    {
        if ($packet = $this->redis->hgetall(self::redisKey($this))) {
            return $packet;
        }

        return false;
    }

    /**
     * Fetch the status for the job
     *
     * @return int Status as as an integer, based on the Job constants
     */
    public function getStatus()
    {
        $status = $this->redis->hget(self::redisKey($this), 'status');
        
        if($status !== null) {
            return (int) $status;
        }

        return false;
    }

    /**
     * Returns formatted execution time string
     *
     * @return string
     */
    public function execTime()
    {
        $packet = $this->getPacket();

        if(empty($packet['started'])) return 0;

        if (!isset($packet['finished']) || $packet['finished'] === 0) {
            throw new \Exception('The job has not yet ran');
        }

        return $packet['finished'] - $packet['started'];
    }

    /**
     * Returns formatted execution time string
     *
     * @return string
     */
    public function execTimeStr()
    {
        $execTime = $this->execTime();

        if ($execTime >= 1) {
            return round($execTime, 1).'s';
        } else {
            return round($execTime * 1000, 2).'ms';
        }
    }

    /**
     * Get the job id.
     *
     * @return string
     */
    public function getId()
    {
        return $this->id;
    }

    /**
     * Set the job id.
     *
     * @throws \RuntimeException
     */
    public function setId()
    {
        throw new \RuntimeException('It is not possible to set job id, you must create a new job');
    }

    /**
     * Get the job queue.
     *
     * @return string
     */
    public function getQueue()
    {
        return $this->queue;
    }

    /**
     * Set the job queue.
     *
     * @throws \RuntimeException
     */
    public function setQueue()
    {
        throw new \RuntimeException('It is not possible to set job queue, you must create a new job');
    }

    /**
     * Get the job class.
     *
     * @return string
     */
    public function getClass()
    {
        return $this->class;
    }

    /**
     * Set the job class.
     *
     * @throws \RuntimeException
     */
    public function setClass()
    {
        throw new \RuntimeException('It is not possible to set job class, you must create a new job');
    }

    /**
     * Get the job data.
     *
     * @return array
     */
    public function getData()
    {
        return $this->data;
    }

    /**
     * Set the job data.
     *
     * @throws \RuntimeException
     */
    public function setData()
    {
        throw new \RuntimeException('It is not possible to set job data, you must create a new job');
    }

    /**
     * Get the job delayed time
     *
     * @return int
     */
    public function getDelayedTime()
    {
        $packet = $this->getPacket();

        if ($packet['delayed'] > 0) {
            return $packet['delayed'];
        }

        return -1;
    }

    /**
     * Get the queue worker interface
     *
     * @return Worker
     */
    public function getWorker()
    {
        return $this->worker;
    }

    /**
     * Set the queue worker interface
     *
     * @param Worker $worker
     */
    public function setWorker(Worker $worker)
    {
        $this->worker = $worker;
    }

    /**
     * Return array representation of this job
     *
     * @return array
     */
    public function toArray()
    {
        $packet = $this->getPacket();

        if(empty($packet['latest_line'])) {
            $packet['latest_line'] = '';
        }
        if(empty($packet['progress'])) {
            $packet['progress'] = 0;
        }

        return array(
            'id'        => (string)$this->id,
            'queue'     => (string)$this->queue,
            'class'     => (string)$this->class,
            'data'      => $this->data,
            'worker'    => (string)$packet['worker'],
            'status'    => (int)$packet['status'],
            'created'   => (float)$packet['created'],
            'updated'   => (float)$packet['updated'],
            'delayed'   => (float)$packet['delayed'],
            'started'   => (float)$packet['started'],
            'finished'  => (float)$packet['finished'],
            'progress'  => (float)$packet['progress'],
            'progress_l'  => (string)$packet['latest_line'],
            'output'    => $packet['output'],
            'exception' => $packet['exception']
        );
    }

    /**
     * Look for any jobs which are running but the worker is dead.
     * Meaning that they are also not running but left in limbo
     *
     * This is a form of garbage collection to handle cases where the
     * server may have been killed and the workers did not die gracefully
     * and therefore leave state information in Redis.
     *
     * @param array $queues list of queues to check
     */
    public static function cleanup(array $queues = array('*'))
    {
        $cleaned = array('zombie' => 0, 'processed' => 0);
        $redis = Redis::instance();

        if (in_array('*', $queues)) {
            $queues = (array)$redis->smembers(Queue::redisKey());
            sort($queues);
        }

        $workers = $redis->smembers(Worker::redisKey());

        foreach ($queues as $queue) {
            $jobs = $redis->zrangebyscore(Queue::redisKey($queue, 'running'), 0, time());

            foreach ($jobs as $payload) {
                $job = self::loadPayload($queue, $payload);
                $packet = $job->getPacket();

                if (!in_array($packet['worker'], $workers)) {
                    $job->fail(new Exception\Zombie);

                    $cleaned['zombie']++;
                }
            }

            $cleaned['processed'] = $redis->zremrangebyscore(Queue::redisKey($queue, 'processed'), 0, time() - \Resque::getConfig('default.expiry_time', \Resque::DEFAULT_EXPIRY_TIME));
        }

        return $cleaned;
    }
}
