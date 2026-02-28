<?php

namespace App;

use App\Commands\Visit;

use function array_fill;
use function chr;
use function count;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function gc_disable;
use function getmypid;
use function implode;
use function min;
use function pack;
use function pcntl_fork;
use function pcntl_wait;
use function str_replace;
use function strrpos;
use function strpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;

use const SEEK_CUR;
use const WNOHANG;

final class Parser
{
    private const BUFFER_SIZE   = 4 * 1024 * 1024;
    private const DISCOVER_SIZE = 2 * 1024 * 1024;
    private const PREFIX_LEN    = 25;
    private const WORKERS       = 6;

    public function parse($inputPath, $outputPath)
    {
        gc_disable();

        $fileSize   = filesize($inputPath);
        $numWorkers = self::WORKERS;

        // --- precompute date IDs ---
        $dateIds   = [];
        $dates     = [];
        $dateCount = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2           => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default     => 31,
                };
                $mStr  = $m < 10 ? '0' . $m : (string)$m;
                $ymStr = $y . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $key           = $ymStr . ($d < 10 ? '0' . $d : $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        // --- precompute bytes for faster writes ---
        $dateIdBytes = [];
        foreach ($dateIds as $date => $id) {
            $dateIdBytes[$date] = chr($id & 0xFF) . chr($id >> 8);
        }

        // --- read first chunk to discover paths ---
        $handle = fopen($inputPath, 'rb');
        $raw    = fread($handle, min(self::DISCOVER_SIZE, $fileSize));
        fclose($handle);

        $pathIds   = [];
        $paths     = [];
        $pathCount = 0;
        $pos       = 0;
        $lastNl    = strrpos($raw, "\n") ?: 0;

        while ($pos < $lastNl) {
            $nl = strpos($raw, "\n", $pos + 52);
            if ($nl === false) break;

            $slug = substr($raw, $pos + self::PREFIX_LEN, $nl - $pos - 51);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }

            $pos = $nl + 1;
        }
        unset($raw);

        // --- include Visit::all() slugs ---
        foreach (Visit::all() as $visit) {
            $slug = substr($visit->uri, self::PREFIX_LEN);
            if (!isset($pathIds[$slug])) {
                $pathIds[$slug] = $pathCount;
                $paths[$pathCount] = $slug;
                $pathCount++;
            }
        }

        // --- compute chunk split points for workers ---
        $splitPoints = [0];
        $bh = fopen($inputPath, 'rb');
        for ($i = 1; $i < $numWorkers; $i++) {
            fseek($bh, (int)($fileSize * $i / $numWorkers));
            fgets($bh);
            $splitPoints[] = ftell($bh);
        }
        fclose($bh);
        $splitPoints[] = $fileSize;

        $tmpDir   = sys_get_temp_dir();
        $myPid    = getmypid();
        $childMap = [];

        // --- spawn workers ---
        for ($w = 0; $w < $numWorkers - 1; $w++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $w;
            $pid     = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException('pcntl_fork failed');

            if ($pid === 0) {
                $wCounts = $this->processChunk(
                    $inputPath,
                    $splitPoints[$w],
                    $splitPoints[$w + 1],
                    $pathIds,
                    $dateIdBytes,
                    $pathCount,
                    $dateCount,
                    $dateIds // <-- pass dateIds
                );

                file_put_contents($tmpFile, pack('v*', ...$wCounts));
                exit(0);
            }

            $childMap[$pid] = $tmpFile;
        }

        // --- parent processes last chunk ---
        $counts = $this->processChunk(
            $inputPath,
            $splitPoints[$numWorkers - 1],
            $splitPoints[$numWorkers],
            $pathIds,
            $dateIdBytes,
            $pathCount,
            $dateCount,
            $dateIds // <-- pass dateIds
        );

        // --- collect child results ---
        $pending = count($childMap);
        while ($pending > 0) {
            $pid = pcntl_wait($status, WNOHANG);
            if ($pid <= 0) $pid = pcntl_wait($status);
            if (!isset($childMap[$pid])) continue;

            $tmpFile = $childMap[$pid];
            $wCounts = unpack('v*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) $counts[$j++] += $v;
            $pending--;
        }

        $this->writeJson($outputPath, $counts, $paths, $dates, $dateCount);
    }

    private function processChunk($inputPath, $start, $end, $pathIds, $dateIdBytes, $pathCount, $dateCount, $dateIds)
    {
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        fseek($handle, $start);
        $remaining = $end - $start;

        $bufSize   = self::BUFFER_SIZE;
        $prefixLen = self::PREFIX_LEN;

        $buffer = '';
        while ($remaining > 0) {
            $toRead = min($bufSize, $remaining);
            $chunk  = fread($handle, $toRead);
            if ($chunk === false || $chunk === '') break;

            $buffer .= $chunk;
            $remaining -= strlen($chunk);

            // process complete lines
            $lastNl = strrpos($buffer, "\n");
            if ($lastNl === false) continue; // read more

            $lines = explode("\n", substr($buffer, 0, $lastNl));
            $buffer = substr($buffer, $lastNl + 1); // remainder

            foreach ($lines as $line) {
                $offset = $prefixLen;
                $lineLen = strlen($line);
                while ($offset < $lineLen) {
                    $sep = strpos($line, ',', $offset);
                    if ($sep === false) break;

                    $slug = substr($line, $offset, $sep - $offset);
                    $dateStr = substr($line, $sep + 3, 8);
                    $did = $dateIds[$dateStr] ?? null;
                    if ($did !== null) {
                        $counts[$pathIds[$slug] * $dateCount + $did]++;
                    }

                    $offset = $sep + 52; // move to next entry
                }
            }
        }

        fclose($handle);
        return $counts;
    }

    private function writeJson($outputPath, $counts, $paths, $dates, $dateCount)
    {
        $out = fopen($outputPath, 'wb');
        $pathCount = count($paths);

        $escapedPaths = [];
        for ($p = 0; $p < $pathCount; $p++) {
            $escapedPaths[$p] = "\"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . '"';
        }

        fwrite($out, '{');
        $firstPath = true;

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $dateEntries = [];
            for ($d = 0; $d < $dateCount; $d++) {
                $count = $counts[$base + $d];
                if ($count !== 0) $dateEntries[] = '        "20' . $dates[$d] . '": ' . $count;
            }
            if (empty($dateEntries)) continue;

            $sep = $firstPath ? '' : ',';
            $firstPath = false;

            fwrite($out,
                $sep . "\n    " . $escapedPaths[$p] . ": {\n" .
                implode(",\n", $dateEntries) .
                "\n    }"
            );
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}