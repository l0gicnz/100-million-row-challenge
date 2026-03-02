<?php

namespace App;

use App\Commands\Visit;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fread;
use function fseek;
use function fwrite;
use function fopen;
use function fclose;
use function fgets;
use function ftell;
use function pack;
use function unpack;
use function array_fill;
use function implode;
use function str_replace;
use function count;
use function gc_disable;
use function getmypid;
use function pcntl_fork;
use function pcntl_wait;
use function pcntl_signal;
use function pcntl_async_signals;
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function set_error_handler;
use function restore_error_handler;
use function stream_set_read_buffer;
use function stream_set_write_buffer;

use const SEEK_CUR;
use const SIG_DFL;
use const SIGTERM;
use const SIGINT;

final class Parser
{
    private const int CHUNK_SIZE    = 2_097_152;
    private const int READ_BUFFER   = 1_048_576;
    private const int WORKER_COUNT  = 8;
    private const int SEGMENT_COUNT = 32;
    private const int URI_OFFSET    = 25;
    private const int FILE_SIZE     = 7_509_674_827;

    private array $childPids = [];

    private ?array $ipcQueue   = null;
    private ?array $ipcShmConf = null;

    public static function parse(string $source, string $destination): void
    {
        gc_disable();
        (new self())->execute($source, $destination);
    }

    private function execute(string $input, string $output): void
    {
        [$dateIds, $dateList] = $this->buildDateRegistry();
        $dateCount = count($dateList);

        $slugs     = $this->discoverSlugs($input);
        $slugCount = count($slugs);

        $slugMap = [];
        foreach ($slugs as $id => $slug) {
            $slugMap[$slug] = $id * $dateCount;
        }

        $boundaries = $this->calculateSplits($input);
        $shmConfig  = $this->setupSharedMemory($slugCount, $dateCount);
        $queue      = $this->initWorkQueue();

        $this->ipcQueue   = $queue;
        $this->ipcShmConf = $shmConfig;
        $this->registerShutdownHandlers();

        $pids = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException("pcntl_fork() failed for worker $i");
            }

            if ($pid === 0) {
                pcntl_signal(SIGTERM, SIG_DFL);
                pcntl_signal(SIGINT,  SIG_DFL);

                $this->runWorker($input, $boundaries, $slugMap, $dateIds, $dateCount, $slugCount, $queue, $shmConfig, $i);
                exit(0);
            }

            $pids[$pid]          = $i;
            $this->childPids[$i] = $pid;
        }

        $aggregated = array_fill(0, $slugCount * $dateCount, 0);
        $this->consumeQueue($input, $boundaries, $slugMap, $dateIds, $queue, $aggregated);

        while ($pids) {
            $pid = pcntl_wait($status);
            if ($pid <= 0) continue;

            $workerIdx    = $pids[$pid];
            unset($pids[$pid], $this->childPids[$workerIdx]);

            $workerData   = $this->retrieveWorkerResult($shmConfig, $workerIdx);
            $workerCounts = array_values(unpack('v*', $workerData));
            $total        = $slugCount * $dateCount;

            for ($j = 0; $j < $total; $j++) {
                $aggregated[$j] += $workerCounts[$j];
            }
        }

        $this->cleanupIPC($queue);
        $this->ipcQueue = $this->ipcShmConf = null;

        $this->generateJson($output, $aggregated, $slugs, $dateList);
    }

    private function registerShutdownHandlers(): void
    {
        pcntl_async_signals(true);

        $handler = function (int $sig): void {
            foreach ($this->childPids as $pid) {
                @posix_kill($pid, SIGTERM);
            }
            foreach ($this->childPids as $pid) {
                @pcntl_waitpid($pid, $status);
            }
            if ($this->ipcQueue !== null) {
                $this->cleanupIPC($this->ipcQueue);
                $this->ipcQueue = null;
            }
            exit(1);
        };

        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT,  $handler);
    }

    private function runWorker($path, $splits, $slugMap, $dateIds, $dateCount, $slugCount, $queue, $shm, $idx): void
    {
        $counts = array_fill(0, $slugCount * $dateCount, 0);
        $this->consumeQueue($path, $splits, $slugMap, $dateIds, $queue, $counts);
        $packed = pack('v*', ...$counts);

        if ($shm['handles'][$idx] === false) {
            throw new \RuntimeException("Shared memory handle for worker $idx is invalid.");
        }

        shmop_write($shm['handles'][$idx], $packed, 0);
    }

    private function consumeQueue($path, $splits, $slugMap, $dateIds, $queue, &$counts): void
    {
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        while (($chunkIdx = $this->nextJob($queue)) !== -1) {
            $this->parseRange($fh, $splits[$chunkIdx], $splits[$chunkIdx + 1], $slugMap, $dateIds, $counts);
        }
        fclose($fh);
    }

    private function parseRange($fh, $start, $end, $slugMap, $dateIds, &$counts): void
    {
        fseek($fh, $start);
        $remaining = $end - $start;
        $bufSize   = self::READ_BUFFER;

        while ($remaining > 0) {
            $buffer = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
            if ($buffer === false || $buffer === '') break;

            $len       = strlen($buffer);
            $remaining -= $len;
            $lastNl    = strrpos($buffer, "\n");

            if ($lastNl === false) break;

            $overhang = $len - $lastNl - 1;
            if ($overhang > 0) {
                fseek($fh, -$overhang, SEEK_CUR);
                $remaining += $overhang;
            }

            $p     = self::URI_OFFSET;
            $fence = $lastNl - (16 * (49 + 52)); // 1616

            while ($p < $fence) {
                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }
        }
    }

    private function nextJob(array $queue): int
    {
        sem_acquire($queue['sem']);
        $val = unpack('V', shmop_read($queue['shm'], 0, 4))[1];
        if ($val < self::SEGMENT_COUNT) {
            shmop_write($queue['shm'], pack('V', $val + 1), 0);
        } else {
            $val = -1;
        }
        sem_release($queue['sem']);
        return $val;
    }

    private function shmopCreate(int $key, int $size): \Shmop
    {
        set_error_handler(static fn() => true);

        $handle = @shmop_open($key, 'n', 0644, $size);

        if ($handle === false) {
            $stale = @shmop_open($key, 'w', 0, 0);
            if ($stale !== false) {
                shmop_delete($stale);
            }
            $handle = @shmop_open($key, 'n', 0644, $size);
        }

        restore_error_handler();

        if ($handle === false) {
            throw new \RuntimeException(
                "shmop_open() failed for key $key (size $size). " .
                "Check kernel shmmax: `sysctl kern.sysv.shmmax` on macOS."
            );
        }

        return $handle;
    }

    private function setupSharedMemory(int $sCount, int $dCount): array
    {
        $size    = $sCount * $dCount * 2;
        $pid     = getmypid();
        $handles = [];

        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $handles[$i] = $this->shmopCreate($pid + 100 + $i, $size);
        }

        return ['handles' => $handles, 'size' => $size];
    }

    private function initWorkQueue(): array
    {
        $pid = getmypid();

        set_error_handler(static fn() => true);

        $sem = @sem_get($pid + 1, 1, 0644, true);
        if ($sem === false) {
            restore_error_handler();
            throw new \RuntimeException("sem_get() failed for key " . ($pid + 1));
        }

        restore_error_handler();

        $shm = $this->shmopCreate($pid + 2, 4);
        shmop_write($shm, pack('V', 0), 0);

        return ['sem' => $sem, 'shm' => $shm];
    }

    private function retrieveWorkerResult(array $config, int $idx): string
    {
        $handle = $config['handles'][$idx];
        $data   = shmop_read($handle, 0, $config['size']);
        shmop_delete($handle);
        return $data;
    }

    private function cleanupIPC(array $queue): void
    {
        @shmop_delete($queue['shm']);
        @sem_remove($queue['sem']);
    }

    private function buildDateRegistry(): array
    {
        $map = []; $list = []; $id = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $maxD; $d++) {
                    $date        = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $key         = substr($date, 1);
                    $map[$key]   = $id;
                    $list[$id++] = $date;
                }
            }
        }
        return [$map, $list];
    }

    private function discoverSlugs(string $path): array
    {
        $fh  = fopen($path, 'rb');
        $raw = fread($fh, self::CHUNK_SIZE);
        fclose($fh);

        $slugs = [];
        $pos   = 0;
        $limit = strrpos($raw, "\n") ?: 0;
        while ($pos < $limit) {
            $eol = strpos($raw, "\n", $pos + 52);
            if ($eol === false) break;
            $slugs[substr($raw, $pos + self::URI_OFFSET, $eol - $pos - 51)] = true;
            $pos = $eol + 1;
        }

        foreach (Visit::all() as $v) {
            $slugs[substr($v->uri, self::URI_OFFSET)] = true;
        }
        return array_keys($slugs);
    }

    private function calculateSplits(string $path): array
    {
        $pts     = [0];
        $fh      = fopen($path, 'rb');
        $segSize = (int) (self::FILE_SIZE / self::SEGMENT_COUNT);

        for ($i = 1; $i < self::SEGMENT_COUNT; $i++) {
            fseek($fh, $i * $segSize);
            fgets($fh);
            $pts[] = ftell($fh);
        }

        fclose($fh);
        $pts[] = self::FILE_SIZE;
        return $pts;
    }

    private function generateJson(string $out, array $counts, array $slugs, array $dates): void
    {
        $fp = fopen($out, 'wb');
        stream_set_write_buffer($fp, 4_194_304);
        fwrite($fp, '{');

        $dCount = count($dates);

        $datePrefixes = [];
        for ($d = 0; $d < $dCount; $d++) {
            $datePrefixes[$d] = "        \"20{$dates[$d]}\": ";
        }

        $escapedSlugs = [];
        foreach ($slugs as $idx => $slug) {
            $escapedSlugs[$idx] = "\"\\/blog\\/" . str_replace('/', '\\/', $slug) . "\"";
        }

        $isFirst = true;
        $base    = 0;
        foreach ($slugs as $sIdx => $_) {
            $entries = [];
            for ($d = 0; $d < $dCount; $d++) {
                if ($val = $counts[$base + $d]) {
                    $entries[] = $datePrefixes[$d] . $val;
                }
            }
            if ($entries) {
                $comma   = $isFirst ? "" : ",";
                $isFirst = false;
                fwrite($fp, "$comma\n    {$escapedSlugs[$sIdx]}: {\n" . implode(",\n", $entries) . "\n    }");
            }
            $base += $dCount;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}