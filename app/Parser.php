<?php

namespace App;

use App\Commands\Visit;
//use RuntimeException;

final class Parser
{
    private const int CHUNK_SIZE = 2_097_152;
    private const int READ_BUFFER = 163_840;
    private const int WORKER_COUNT = 8;
    private const int SEGMENT_COUNT = 16;
    private const int URI_OFFSET = 25;

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
        $fileSize = self::FILE_SIZE;

        [$dateMap, $dateList] = $this->buildDateRegistry();
        $dateIdBinary = [];
        foreach ($dateMap as $date => $id) {
            $dateIdBinary[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        $slugs = $this->discoverSlugs($input, $fileSize);
        $slugMap = array_flip($slugs);
        $slugCount = count($slugs);
        $dateCount = count($dateList);

        $boundaries = $this->calculateSplits($input, $fileSize);

        $shmConfig = $this->setupSharedMemory($slugCount, $dateCount);
        $queue = $this->initWorkQueue();

        $pids = [];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            $pid = pcntl_fork();
            if ($pid === -1) throw new RuntimeException("Fork failed");

            if ($pid === 0) {
                $this->runWorker($input, $boundaries, $slugMap, $dateIdBinary, $queue, $shmConfig, $i);
                exit(0);
            }
            $pids[$pid] = $i;
        }

        $localBuckets = array_fill(0, $slugCount, '');
        $this->consumeQueue($input, $boundaries, $slugMap, $dateIdBinary, $queue, $localBuckets);
        $aggregated = $this->processBuckets($localBuckets, $slugCount, $dateCount);

        while (count($pids) > 0) {
            $pid = pcntl_wait($status);
            if (!isset($pids[$pid])) continue;
            
            $workerIdx = $pids[$pid];
            unset($pids[$pid]);

            $workerData = $this->retrieveWorkerResult($shmConfig, $workerIdx);
            $workerCounts = unpack('v*', $workerData);
            
            $totalCount = count($workerCounts);
            //for ($j = 1; $j <= $totalCount; $j++) {
            //    $aggregated[$j - 1] += $workerCounts[$j];
            //}
            for ($j = 0; $j < $totalCount; $j++) {
                $aggregated[$j] += $workerCounts[$j + 1];
            }
        }

        $this->cleanupIPC($shmConfig, $queue);
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
                    $date = sprintf("%02d-%02d-%02d", $y, $m, $d);
                    $map[$date] = $id;
                    $list[$id++] = $date;
                }
            }
        }
        return [$map, $list];
    }

    private function discoverSlugs(string $path, int $size): array
    {
        $fh = fopen($path, 'rb');
        $raw = fread($fh, min(self::CHUNK_SIZE, $size));
        fclose($fh);

        $slugs = [];
        $pos = 0;
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

    private function runWorker($path, $splits, $slugMap, $dateBytes, $queue, $shm, $idx): void
    {
        $buckets = array_fill(0, count($slugMap), '');
        $this->consumeQueue($path, $splits, $slugMap, $dateBytes, $queue, $buckets);
        
        $counts = $this->processBuckets($buckets, count($slugMap), count($dateBytes));
        $packed = pack('v*', ...$counts);

        if ($shm['enabled']) {
            shmop_write($shm['handles'][$idx], $packed, 0);
        } else {
            file_put_contents($shm['temp_prefix'] . $idx, $packed);
        }
    }

    private function consumeQueue($path, $splits, $slugMap, $dateBytes, $queue, &$buckets): void
    {
        $fh = fopen($path, 'rb');
        stream_set_read_buffer($fh, 0);
        while (($chunkIdx = $this->nextJob($queue)) !== -1) {
            $this->parseRange($fh, $splits[$chunkIdx], $splits[$chunkIdx + 1], $slugMap, $dateBytes, $buckets);
        }
        fclose($fh);
    }

    private function parseRange($fh, $start, $end, $slugMap, $dateBytes, &$buckets): void
    {
        fseek($fh, $start);
        $remaining = $end - $start;
        $bufSize = self::READ_BUFFER;

        while ($remaining > 0) {
            //$buffer = fread($fh, min($remaining, $bufSize));
            $buffer = fread($fh, $remaining > $bufSize ? $bufSize : $remaining);
            if ($buffer === false || $buffer === '') break;
            
            $len = strlen($buffer);
            $remaining -= $len;
            $lastNl = strrpos($buffer, "\n");
            if ($lastNl === false) continue;

            $overhang = $len - $lastNl - 1;
            if ($overhang > 0) {
                fseek($fh, -$overhang, SEEK_CUR);
                $remaining += $overhang;
            }

            $p = self::URI_OFFSET;
            $fence = $lastNl - 792;

            while ($p < $fence) {
                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p);
                if ($comma === false || $comma >= $lastNl) break;
                $buckets[$slugMap[substr($buffer, $p, $comma - $p)]] .= $dateBytes[substr($buffer, $comma + 3, 8)];
                $p = $comma + 52;
            }
        }
    }

    private function processBuckets(array &$buckets, int $slugCount, int $dateCount): array
    {
        $results = array_fill(0, $slugCount * $dateCount, 0);
        foreach ($buckets as $id => $data) {
            if ($data === '') continue;
            $base = $id * $dateCount;
            foreach (array_count_values(unpack('v*', $data)) as $dateId => $count) {
                $results[$base + $dateId] = $count;
            }
        }
        return $results;
    }

    private function nextJob(array $queue): int
    {
        if ($queue['type'] === 'sem') {
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
        flock($queue['fh'], LOCK_EX);
        fseek($queue['fh'], 0);
        $val = unpack('V', fread($queue['fh'], 4))[1];
        if ($val < self::SEGMENT_COUNT) {
            fseek($queue['fh'], 0);
            fwrite($queue['fh'], pack('V', $val + 1));
        } else {
            $val = -1;
        }
        flock($queue['fh'], LOCK_UN);
        return $val;
    }

    private function calculateSplits(string $path, int $size): array
    {
        $pts = [0];
        $fh = fopen($path, 'rb');
        foreach (self::SPLIT_OFFSETS as $offset) {
            fseek($fh, $offset);
            fgets($fh);
            $pts[] = ftell($fh);
        }
        fclose($fh);
        $pts[] = $size;
        return $pts;
    }

    private function setupSharedMemory(int $sCount, int $dCount): array
    {
        $size = $sCount * $dCount * 2;
        $pid = getmypid();
        $config = ['enabled' => true, 'handles' => [], 'temp_prefix' => sys_get_temp_dir() . "/p100_$pid"];
        for ($i = 0; $i < self::WORKER_COUNT - 1; $i++) {
            set_error_handler(null);
            $shm = @shmop_open($pid + 100 + $i, 'c', 0644, $size);
            set_error_handler(null);
            if (!$shm) { $config['enabled'] = false; break; }
            $config['handles'][$i] = $shm;
        }
        return $config;
    }

    private function initWorkQueue(): array
    {
        $pid = getmypid();
        set_error_handler(null);
        $sem = @sem_get($pid + 1, 1, 0644, true);
        $shm = @shmop_open($pid + 2, 'c', 0644, 4);
        set_error_handler(null);
        if ($sem && $shm) {
            shmop_write($shm, pack('V', 0), 0);
            return ['type' => 'sem', 'sem' => $sem, 'shm' => $shm];
        }
        $f = sys_get_temp_dir() . "/q_$pid";
        file_put_contents($f, pack('V', 0));
        return ['type' => 'file', 'fh' => fopen($f, 'c+b'), 'path' => $f];
    }

    private function retrieveWorkerResult(array $config, int $idx): string
    {
        if ($config['enabled']) {
            $data = shmop_read($config['handles'][$idx], 0, 0);
            shmop_delete($config['handles'][$idx]);
            return $data;
        }
        $path = $config['temp_prefix'] . $idx;
        $data = file_get_contents($path);
        @unlink($path);
        return $data;
    }

    private function cleanupIPC(array $shm, array $queue): void
    {
        if ($queue['type'] === 'sem') {
            shmop_delete($queue['shm']);
            sem_remove($queue['sem']);
        } else {
            fclose($queue['fh']);
            @unlink($queue['path']);
        }
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
        foreach ($slugs as $sIdx => $slug) {
            $entries = [];
            $offset = $sIdx * $dCount;
            for ($d = 0; $d < $dCount; $d++) {
                if ($val = $counts[$offset + $d]) {
                    $entries[] = $datePrefixes[$d] . $val;
                }
            }
            if (!$entries) continue;

            $comma = $isFirst ? "" : ",";
            $isFirst = false;
            fwrite($fp, "$comma\n    {$escapedSlugs[$sIdx]}: {\n" . implode(",\n", $entries) . "\n    }");
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}