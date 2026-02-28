<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function count;
use function fclose;
use function fflush;
use function fgets;
use function filesize;
use function flock;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function gc_disable;
use function getmypid;
use function intdiv;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function posix_kill;
use function preg_match_all;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function str_replace;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;

use const LOCK_EX;
use const LOCK_UN;
use const SEEK_CUR;
use const SIGKILL;
use const WNOHANG;

final class Parser
{
    private const int WORKERS    = 8;
    private const int CHUNKS     = 16;
    private const int READ_CHUNK = 1_048_576; // Increased to 1MB for M1 NVMe
    private const int DISC_SIZE  = 2_097_152;
    private const int PREFIX_LEN = 25;

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath): void
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // 1. Build Date Mappings (Int IDs instead of Binary Strings)
        $dateMap   = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            $shortYear = (string)($y % 10);
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $yStr = ($y < 10 ? '0' : '') . $y;
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $key = $shortYear . '-' . $mStr . '-' . $dStr;
                    $dateMap[$key] = $dateCount; // Map to integer ID
                    $dates[$dateCount] = '20' . $yStr . '-' . $mStr . '-' . $dStr;
                    $dateCount++;
                }
            }
        }

        // 2. Discover Paths
        $dh = fopen($inputPath, 'rb');
        $raw = fread($dh, min(self::DISC_SIZE, $fileSize));
        fclose($dh);

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;

        preg_match_all('/^.{25}([^,]++),/m', $raw, $m);
        foreach ($m[1] as $slug) {
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }
        unset($raw, $m);

        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // 3. Precompute Output Prefixes
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '": {';
        }

        // 4. Multi-processing Setup
        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::CHUNKS; $i++) {
            fseek($bh, intdiv($fileSize * $i, self::CHUNKS));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $myPid     = getmypid();
        $queueFile = sys_get_temp_dir() . '/p100m_' . $myPid . '_queue';
        $qf        = fopen($queueFile, 'c+b');
        fwrite($qf, pack('V', 0));
        fflush($qf);

        // Results stored as uint32 (4 bytes) per cell to avoid overflow in workers
        $shmSize = $pathCount * $dateCount * 4;
        $shms    = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shms[$w] = shmop_open(0x50485000 + ($myPid & 0xFFFF) * 16 + $w, 'c', 0600, $shmSize);
        }

        $childMap = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $fh = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $myQf = fopen($queueFile, 'c+b');

                // The "Packed Array" - O(1) access, no string concat
                $counts = array_fill(0, $pathCount * $dateCount, 0);

                while (true) {
                    $ci = self::grabChunk($myQf, self::CHUNKS);
                    if ($ci === -1) break;
                    self::fillCounts($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $dateMap, $pathIds, $dateCount, $counts);
                }

                shmop_write($shms[$w], pack('V*', ...$counts), 0);
                posix_kill(posix_getpid(), SIGKILL);
            }
            $childMap[$pid] = $w;
        }

        // Parent Work
        $fh = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);
        $totalCounts = array_fill(0, $pathCount * $dateCount, 0);

        while (true) {
            $ci = self::grabChunk($qf, self::CHUNKS);
            if ($ci === -1) break;
            self::fillCounts($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $dateMap, $pathIds, $dateCount, $totalCounts);
        }

        fclose($qf); fclose($fh); unlink($queueFile);

        // Merge Child Results
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;
            $w = $childMap[$pid];
            $wCounts = unpack('V*', shmop_read($shms[$w], 0, $shmSize));
            shmop_delete($shms[$w]);
            
            foreach ($wCounts as $idx => $v) {
                $totalCounts[$idx - 1] += $v;
            }
            $pending--;
        }

        self::writeJson($outputPath, $totalCounts, $pathPrefixes, $datePrefixes, $pathCount, $dateCount);
    }

    private static function grabChunk($qf, int $numChunks): int
    {
        flock($qf, LOCK_EX);
        fseek($qf, 0);
        $idx = unpack('V', fread($qf, 4))[1];
        if ($idx >= $numChunks) {
            flock($qf, LOCK_UN);
            return -1;
        }
        fseek($qf, 0);
        fwrite($qf, pack('V', $idx + 1));
        fflush($qf);
        flock($qf, LOCK_UN);
        return $idx;
    }

    private static function fillCounts($handle, int $start, int $end, array $dateMap, array $pathIds, int $dateCount, array &$counts): void
    {
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk = fread($handle, $toRead);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;
            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $p = self::PREFIX_LEN;
            $fence = $lastNl - 900;

            // Re-using your successful strpos unrolling but with integer table incrementing
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;
            }

            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $id  = $pathIds[substr($chunk, $p, $sep - $p)];
                $did = $dateMap[substr($chunk, $sep + 4, 7)];
                $counts[$id * $dateCount + $did]++;
                $p = $sep + 52;
            }
        }
    }

    private static function writeJson(
        string $outputPath, array $counts,
        array $pathPrefixes, array $datePrefixes,
        int $pathCount, int $dateCount
    ): void {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $buf       = '{';
        $firstPath = true;
        $base      = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $dateBuf = '';
            $sep     = "\n";

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateBuf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($dateBuf !== '') {
                $buf      .= ($firstPath ? '' : ',') . $pathPrefixes[$p] . $dateBuf . "\n    }";
                $firstPath = false;
                if (strlen($buf) > 65536) {
                    fwrite($out, $buf);
                    $buf = '';
                }
            }
            $base += $dateCount;
        }

        fwrite($out, $buf . "\n}");
        fclose($out);
    }
}