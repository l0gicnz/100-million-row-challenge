<?php

namespace App;

use function strpos;
use function strrpos;
use function substr;
use function strlen;
use function fread;
use function fseek;
use function fwrite;
use function fopen;
use function fclose;
use function filesize;
use function implode;
use function str_replace;
use function count;
use function array_fill;
use function gc_disable;
use function stream_set_read_buffer;
use function stream_set_write_buffer;

use const SEEK_CUR;

final class Parser
{
    private const int READ_CHUNK  = 196_608;
    private const int DISC_READ   = 4_194_304;
    private const int WRITE_BUF   = 4_194_304;
    private const int URI_OFFSET  = 25;
    private const int LOOP_FENCE  = 1010;
    private const int MIN_SLUG_LEN = 4;

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

        $counts = array_fill(0, $slugCount * $dateCount, 0);

        $fileSize = filesize($input);

        $fh = fopen($input, 'rb');
        stream_set_read_buffer($fh, 0);
        $this->parseRange($fh, 0, $fileSize, $slugMap, $dateIds, $counts);
        fclose($fh);

        $this->generateJson($output, $counts, $slugs, $dateList);
    }

    private function buildDateRegistry(): array
    {
        $map = []; $list = []; $id = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) {
                    2       => ($y === 24) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                $mStr = ($m < 10 ? '0' : '') . $m;
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr        = ($d < 10 ? '0' : '') . $d;
                    $key         = ($y % 10) . '-' . $mStr . '-' . $dStr;
                    $map[$key]   = $id;
                    $list[$id++] = "20{$y}-{$mStr}-{$dStr}";
                }
            }
        }
        return [$map, $list];
    }

    private function discoverSlugs(string $path): array
    {
        $fh  = fopen($path, 'rb');
        $raw = fread($fh, self::DISC_READ);
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

        return array_keys($slugs);
    }

    private function parseRange($fh, int $start, int $end, array $slugMap, array $dateIds, array &$counts): void
    {
        fseek($fh, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $buffer = fread($fh, $remaining > self::READ_CHUNK ? self::READ_CHUNK : $remaining);
            if ($buffer === false || $buffer === '') break;

            $len        = strlen($buffer);
            $remaining -= $len;
            $lastNl     = strrpos($buffer, "\n");
            if ($lastNl === false) break;

            $overhang = $len - $lastNl - 1;
            if ($overhang > 0) {
                fseek($fh, -$overhang, SEEK_CUR);
                $remaining += $overhang;
            }

            $p     = self::URI_OFFSET;
            $fence = $lastNl - self::LOOP_FENCE;

            while ($p < $fence) {
                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p + self::MIN_SLUG_LEN);
                if ($comma === false || $comma >= $lastNl) break;
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }
        }
    }

    private function generateJson(string $out, array $counts, array $slugs, array $dates): void
    {
        $fp = fopen($out, 'wb');
        stream_set_write_buffer($fp, self::WRITE_BUF);

        $dCount = count($dates);

        $datePrefixes = [];
        for ($d = 0; $d < $dCount; $d++) {
            $datePrefixes[$d] = "        \"{$dates[$d]}\": ";
        }

        $escapedSlugs = [];
        foreach ($slugs as $idx => $slug) {
            $escapedSlugs[$idx] = "\"\\/blog\\/" . str_replace('/', '\\/', $slug) . "\"";
        }

        $buf     = '{';
        $bufLen  = 1;
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
                $comma    = $isFirst ? '' : ',';
                $isFirst  = false;
                $entry    = "$comma\n    {$escapedSlugs[$sIdx]}: {\n" . implode(",\n", $entries) . "\n    }";
                $buf     .= $entry;
                $bufLen  += strlen($entry);

                if ($bufLen > 65_536) {
                    fwrite($fp, $buf);
                    $buf    = '';
                    $bufLen = 0;
                }
            }
            $base += $dCount;
        }

        fwrite($fp, $buf . "\n}");
        fclose($fp);
    }
}