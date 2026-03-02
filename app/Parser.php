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
use function sem_acquire;
use function sem_get;
use function sem_release;
use function sem_remove;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function set_error_handler;
use function stream_set_read_buffer;
use function stream_set_write_buffer;

use const SEEK_CUR;

final class Parser
{
    private const int CHUNK_SIZE    = 2_097_152;
    private const int READ_BUFFER   = 163_840;
    private const int WORKER_COUNT  = 8;
    private const int SEGMENT_COUNT = 16;
    private const int URI_OFFSET    = 25;

    private const array SPLIT_OFFSETS = [
        469_354_676,
        938_709_353,
        1_408_064_029,
        1_877_418_706,
        2_346_773_382,
        2_816_128_059,
        3_285_482_735,
        3_754_837_412,
        4_224_192_088,
        4_693_546_765,
        5_162_901_441,
        5_632_256_118,
        6_101_610_794,
        6_570_965_471,
        7_040_320_147,
    ];

    private const int FILE_SIZE = 7_509_674_827;

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

        $pids = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $pid = pcntl_fork();

            if ($pid === 0) {
                $this->runWorker($input, $boundaries, $slugMap, $dateIds, $dateCount, $slugCount, $queue, $shmConfig, $i);
                exit(0);
            }
            $pids[$pid] = $i;
        }

        $aggregated = array_fill(0, $slugCount * $dateCount, 0);
        $this->consumeQueue($input, $boundaries, $slugMap, $dateIds, $queue, $aggregated);

        while ($pids) {
            $pid = pcntl_wait($status);

            $workerIdx    = $pids[$pid];
            unset($pids[$pid]);

            $workerData   = $this->retrieveWorkerResult($shmConfig, $workerIdx);
            $workerCounts = unpack('v*', $workerData);

            for ($j = 0; $j < $slugCount * $dateCount; $j++) {
                $aggregated[$j] += $workerCounts[$j + 1];
            }
        }

        $this->cleanupIPC($queue);
        $this->generateJson($output, $aggregated, $slugs, $dateList);
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
                    // Store 7-char key (strip leading '2' from '2y-mm-dd' → 'y-mm-dd')
                    // Matches substr($buffer, $comma + 4, 7) extraction in parseRange
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

    private function runWorker($path, $splits, $slugMap, $dateIds, $dateCount, $slugCount, $queue, $shm, $idx): void
    {
        $counts = array_fill(0, $slugCount * $dateCount, 0);
        $this->consumeQueue($path, $splits, $slugMap, $dateIds, $queue, $counts);
        $packed = pack('v*', ...$counts);

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
            $fence = $lastNl - 792;

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

    private function calculateSplits(string $path): array
    {
        $pts = [0];
        $fh  = fopen($path, 'rb');
        foreach (self::SPLIT_OFFSETS as $offset) {
            fseek($fh, $offset);
            fgets($fh);
            $pts[] = ftell($fh);
        }
        fclose($fh);
        $pts[] = self::FILE_SIZE;
        return $pts;
    }

    private function setupSharedMemory(int $sCount, int $dCount): array
    {
        $size    = $sCount * $dCount * 2;
        $pid     = getmypid();
        $handles = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            set_error_handler(null);
            $handles[$i] = @shmop_open($pid + 100 + $i, 'c', 0644, $size);
            set_error_handler(null);
        }
        return ['handles' => $handles];
    }

    private function initWorkQueue(): array
    {
        $pid = getmypid();
        set_error_handler(null);
        $sem = @sem_get($pid + 1, 1, 0644, true);
        $shm = @shmop_open($pid + 2, 'c', 0644, 4);
        set_error_handler(null);
        shmop_write($shm, pack('V', 0), 0);
        return ['sem' => $sem, 'shm' => $shm];
    }

    private function retrieveWorkerResult(array $config, int $idx): string
    {
        $data = shmop_read($config['handles'][$idx], 0, 0);
        shmop_delete($config['handles'][$idx]);
        return $data;
    }

private function cleanupIPC(array $queue): void
{
    shmop_delete($queue['shm']);
    sem_remove($queue['sem']);
}

    private function generateJson(string $out, array $counts, array $slugs, array $dates): void
    {
        $fp = fopen($out, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
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