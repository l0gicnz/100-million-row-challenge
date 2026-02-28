<?php

namespace App;

use App\Commands\Visit;

use function array_count_values;
use function array_fill;
use function chr;
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
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function sprintf;
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
use const WNOHANG;

final class Parser
{
    private const int WORKERS      = 8;          // Matches M1's 8 real cores
    private const int CHUNKS       = 16;         // More chunks than workers = better load balancing
    private const int READ_CHUNK   = 524_288;    // 512 KB read buffer
    private const int DISC_SIZE    = 2_097_152;  // 2 MB for path discovery
    private const int PREFIX_LEN   = 25;         // Length of "https://stitcher.io/blog/"

    public function __call(string $name, array $arguments): mixed
    {
        return static::$name(...$arguments);
    }

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize = filesize($inputPath);

        // Build date mappings — years 21–26
        $dateChars = [];
        $dates     = [];
        $dateCount = 0;

        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => $y === 24 ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = ($m < 10 ? '0' : '') . $m;
                $dBase = ($y < 10 ? '0' : '') . $y . '-' . $mStr . '-';
                // Key is 7 chars ("6-01-24") — skip the shared leading "2" of "202X"
                // matching substr($chunk, $sep + 4, 7) in the hot loop
                $shortYear = (string)($y % 10);
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr                  = ($d < 10 ? '0' : '') . $d;
                    $key                   = $shortYear . '-' . $mStr . '-' . $dStr;
                    $dateChars[$key]        = chr($dateCount & 0xFF) . chr($dateCount >> 8);
                    $dates[$dateCount]      = '20' . $dBase . $dStr;
                    $dateCount++;
                }
            }
        }

        // Discover paths from first 2 MB — one preg pass, called once
        $dh  = fopen($inputPath, 'rb');
        $raw = fread($dh, min(self::DISC_SIZE, $fileSize));
        fclose($dh);

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;

        preg_match_all('/^.{25}([^,]++),/m', $raw, $m);
        foreach ($m[1] as $slug) {
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }
        unset($raw, $m);

        // Include Visit::all() paths
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug]    = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // Precompute output strings
        $datePrefixes = [];
        for ($d = 0; $d < $dateCount; $d++) {
            $datePrefixes[$d] = '        "' . $dates[$d] . '": ';
        }

        $pathPrefixes = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $pathPrefixes[$p] = "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '": {';
        }

        // Compute CHUNKS split points
        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::CHUNKS; $i++) {
            fseek($bh, intdiv($fileSize * $i, self::CHUNKS));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        // Shared queue file: a single uint32 = next chunk index to claim
        $myPid     = getmypid();
        $tmpDir    = sys_get_temp_dir();
        $queueFile = $tmpDir . '/p100m_' . $myPid . '_queue';
        $qf        = fopen($queueFile, 'c+b');
        fwrite($qf, pack('V', 0));
        fflush($qf);

        // Pre-allocate shmop segments for child results (2 bytes per uint16)
        $shmSize = $pathCount * $dateCount * 2;
        $shms    = [];
        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $shms[$w] = shmop_open(
                0x50485000 + ($myPid & 0xFFFF) * 16 + $w,
                'c', 0600, $shmSize
            );
        }

        $childMap = []; // pid => worker index

        for ($w = 0; $w < self::WORKERS - 1; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $buckets = array_fill(0, $pathCount, '');
                $fh      = fopen($inputPath, 'rb');
                stream_set_read_buffer($fh, 0);
                $myQf    = fopen($queueFile, 'c+b');

                while (true) {
                    $ci = self::grabChunk($myQf, self::CHUNKS);
                    if ($ci === -1) break;
                    self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateChars, $buckets);
                }

                fclose($myQf);
                fclose($fh);

                $counts = self::bucketsToCounts($buckets, $pathCount, $dateCount);
                shmop_write($shms[$w], pack('v*', ...$counts), 0);
                exit(0);
            }
            $childMap[$pid] = $w;
        }

        // Parent also pulls from the queue
        $buckets = array_fill(0, $pathCount, '');
        $fh      = fopen($inputPath, 'rb');
        stream_set_read_buffer($fh, 0);

        while (true) {
            $ci = self::grabChunk($qf, self::CHUNKS);
            if ($ci === -1) break;
            self::fillBuckets($fh, $splitPoints[$ci], $splitPoints[$ci + 1], $pathIds, $dateChars, $buckets);
        }

        fclose($qf);
        fclose($fh);
        unlink($queueFile);

        $counts  = self::bucketsToCounts($buckets, $pathCount, $dateCount);
        $pending = count($childMap);

        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;
            $w       = $childMap[$pid];
            $wCounts = unpack('v*', shmop_read($shms[$w], 0, $shmSize));
            shmop_delete($shms[$w]);
            $j = 0;
            foreach ($wCounts as $v) $counts[$j++] += $v;
            $pending--;
        }

        self::writeJson($outputPath, $counts, $pathPrefixes, $datePrefixes, $pathCount, $dateCount);
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

    private static function fillBuckets($handle, $start, $end, $pathIds, $dateChars, &$buckets): void
    {
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $toRead   = $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining;
            $chunk    = fread($handle, $toRead);
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

            $p     = self::PREFIX_LEN;
            $fence = $lastNl - 600;

            // 4-line unroll — no inner loop overhead, stays in C for strpos/substr
            while ($p < $fence) {
                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;

                $sep = strpos($chunk, ',', $p);
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }

            // Scalar tail
            while ($p < $lastNl) {
                $sep = strpos($chunk, ',', $p);
                if ($sep === false || $sep >= $lastNl) break;
                $buckets[$pathIds[substr($chunk, $p, $sep - $p)]] .= $dateChars[substr($chunk, $sep + 4, 7)];
                $p = $sep + 52;
            }
        }
    }

    private static function bucketsToCounts(array &$buckets, int $pathCount, int $dateCount): array
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $base   = 0;
        foreach ($buckets as $bucket) {
            if ($bucket !== '') {
                foreach (array_count_values(unpack('v*', $bucket)) as $did => $cnt) {
                    $counts[$base + $did] += $cnt;
                }
            }
            $base += $dateCount;
        }
        return $counts;
    }

    private static function writeJson($outputPath, $counts, $pathPrefixes, $datePrefixes, $pathCount, $dateCount): void
    {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        $buf       = '{';
        $firstPath = true;
        $base      = 0;

        for ($p = 0; $p < $pathCount; $p++) {
            $dateBuf = '';
            $sep     = "\n";   // First entry has no leading comma; subsequent ones do

            for ($d = 0; $d < $dateCount; $d++) {
                $n = $counts[$base + $d];
                if ($n === 0) continue;
                $dateBuf .= $sep . $datePrefixes[$d] . $n;
                $sep = ",\n";
            }

            if ($dateBuf !== '') {
                $buf      .= ($firstPath ? '' : ',') . $pathPrefixes[$p] . $dateBuf . "\n    }";
                $firstPath = false;

                // Flush in userland batches to reduce fwrite syscalls
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