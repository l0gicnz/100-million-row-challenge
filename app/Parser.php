<?php

namespace App;

use App\Commands\Visit;

final class Parser
{
    public function parse($inputPath, $outputPath)
    {
        if (!function_exists('opcache_get_status') || !(opcache_get_status()['jit']['enabled'] ?? false)) {
            // Note: You should run this with -d opcache.enable_cli=1 -d opcache.jit_buffer_size=100M
        }

        $fileSize = \filesize($inputPath);
        
        $cpuCores = 8; 

        [$pathIds, $pathMap, $pathCount, $dateChars, $dateMap, $dateCount] = self::discover($inputPath, $fileSize);

        $workerSlotSize = $pathCount * $dateCount * 4;
        $totalShmSize = $workerSlotSize * $cpuCores;
        $shmId = shmop_open(ftok(__FILE__, 'p'), "c", 0644, $totalShmSize);
        shmop_write($shmId, str_repeat("\0", $totalShmSize), 0);

        $segments = [];
        $fh = \fopen($inputPath, 'rb');
        for ($i = 0; $i < $cpuCores; $i++) {
            $offset = (int)(($fileSize / $cpuCores) * $i);
            if ($i > 0) {
                \fseek($fh, $offset);
                \fgets($fh); 
                $offset = \ftell($fh);
            }
            $segments[$i] = $offset;
        }
        $segments[] = $fileSize;
        \fclose($fh);

        $pids = [];
        for ($i = 0; $i < $cpuCores; $i++) {
            $pid = \pcntl_fork();
            if ($pid === 0) {
                $this->worker($inputPath, $segments[$i], $segments[$i+1], $shmId, $i * $workerSlotSize, $pathIds, $dateChars, $dateCount);
                exit(0);
            }
            $pids[] = $pid;
        }

        foreach ($pids as $pid) \pcntl_waitpid($pid, $status);

        $finalCounts = \array_fill(0, $pathCount * $dateCount, 0);
        for ($i = 0; $i < $cpuCores; $i++) {
            $raw = shmop_read($shmId, $i * $workerSlotSize, $workerSlotSize);
            $data = \unpack('V*', $raw);
            foreach ($data as $idx => $count) {
                if ($count > 0) $finalCounts[$idx - 1] += $count;
            }
        }
        shmop_delete($shmId);

        $this->writeJson($outputPath, $finalCounts, $pathMap, $dateMap, $pathCount, $dateCount);
    }

    private function worker($path, $start, $end, $shmId, $shmOffset, $pathIds, $dateChars, $dateCount)
    {
        $fh = \fopen($path, 'rb');
        \fseek($fh, $start);
        
        $localCounts = \array_fill(0, count($pathIds) * $dateCount, 0);
        
        $bufferSize = 6 * 1024 * 1024; 
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = \fread($fh, \min($remaining, $bufferSize));
            if (!$chunk) break;
            
            $len = \strlen($chunk);
            $remaining -= $len;
            $pos = 0;

            while ($pos < $len) {
                $nl = \strpos($chunk, "\n", $pos);
                if ($nl === false) {
                    $backtrack = $len - $pos;
                    \fseek($fh, -$backtrack, SEEK_CUR);
                    $remaining += $backtrack;
                    break;
                }

                $urlStart = $pos + 25;
                if ($urlStart < $nl) {
                    $comma = \strpos($chunk, ',', $urlStart);
                    if ($comma !== false) {
                        $url = \substr($chunk, $urlStart, $comma - $urlStart);
                        $dateKey = \substr($chunk, $comma + 4, 7);

                        if (isset($pathIds[$url], $dateChars[$dateKey])) {
                            $localCounts[($pathIds[$url] * $dateCount) + $dateChars[$dateKey]]++;
                        }
                    }
                }
                $pos = $nl + 1;
            }
        }
        shmop_write($shmId, \pack('V*', ...$localCounts), $shmOffset);
    }

    private static function discover($inputPath, $fileSize)
    {
        $handle = \fopen($inputPath, 'rb');
        $chunk = \fread($handle, 512000);
        \fclose($handle);

        $pathIds = [];
        $pathCount = 0;
        $minDate = '2026-12-31';
        $maxDate = '2020-01-01';

        $lines = \explode("\n", $chunk);
        foreach ($lines as $line) {
            if (\strlen($line) < 36) continue;
            if (($c = \strpos($line, ',', 25)) === false) continue;
            
            $url = \substr($line, 25, $c - 25);
            if (!isset($pathIds[$url])) $pathIds[$url] = $pathCount++;
            
            $d = \substr($line, $c + 1, 10);
            if ($d < $minDate) $minDate = $d;
            if ($d > $maxDate) $maxDate = $d;
        }

        foreach (Visit::all() as $visit) {
            $url = \substr($visit->uri, 25);
            if (!isset($pathIds[$url])) $pathIds[$url] = $pathCount++;
        }

        $dateChars = [];
        $dateMap = [];
        $curr = \strtotime($minDate < $maxDate ? $minDate : '2020-01-01');
        $end = \strtotime($maxDate > $minDate ? $maxDate : '2026-12-31');
        $dIdx = 0;
        while ($curr <= $end) {
            $full = \date('Y-m-d', $curr);
            $dateChars[\substr($full, 3)] = $dIdx;
            $dateMap[$dIdx++] = $full;
            $curr += 86400;
        }

        return [$pathIds, \array_keys($pathIds), $pathCount, $dateChars, $dateMap, $dIdx];
    }

    private function writeJson($path, $counts, $pathMap, $dateMap, $pCount, $dCount)
    {
        $out = \fopen($path, 'wb');
        \stream_set_write_buffer($out, 1024 * 1024);
        
        \fwrite($out, '{');
        $firstP = true;
        
        for ($p = 0; $p < $pCount; $p++) {
            $inner = '';
            $firstD = true;
            $base = $p * $dCount;
            
            for ($d = 0; $d < $dCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                
                $inner .= ($firstD ? "" : ",\n") . "        \"{$dateMap[$d]}\": " . $count;
                $firstD = false;
            }
            
            if ($inner !== '') {
                $pathJson = ($firstP ? "" : ",") . "\n    \"\\/blog\\/" . \str_replace('/', '\\/', $pathMap[$p]) . "\": {\n" . $inner . "\n    }";
                \fwrite($out, $pathJson);
                $firstP = false;
            }
        }
        
        \fwrite($out, "\n}");
        \fclose($out);
    }
}