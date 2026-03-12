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
use function count;
use function array_fill;
use function filesize;
use function gc_disable;
use function stream_set_read_buffer;
use function stream_set_write_buffer;

use const SEEK_CUR;

final class Parser
{
    private const int DISC_READ   = 4_194_304;
    private const int READ_BUFFER = 1_048_576;
    private const int URI_OFFSET  = 25;
    private const int LOOP_FENCE  = 1010;

    public static function parse($input, $output)
    {
        gc_disable();

        [$dateIds, $dateList] = self::buildDateRegistry();
        $dateCount = count($dateList);

        $slugs     = self::discoverSlugs($input);
        $slugCount = count($slugs);

        $slugMap = [];
        foreach ($slugs as $id => $slug) {
            $slugMap[$slug] = $id * $dateCount;
        }

        $counts = array_fill(0, $slugCount * $dateCount, 0);

        $fh = fopen($input, 'rb');
        stream_set_read_buffer($fh, 0);
        self::parseRange($fh, 0, filesize($input), $slugMap, $dateIds, $counts);
        fclose($fh);

        self::generateJson($output, $counts, $slugs, $dateList);
    }

    private static function buildDateRegistry()
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

    private static function discoverSlugs($path)
    {
        $fh  = fopen($path, 'rb');
        $raw = fread($fh, self::DISC_READ);
        fclose($fh);

        $slugs  = [];
        $pos    = 0;
        $limit  = strrpos($raw, "\n") ?: 0;
        $noNew  = 0;

        while ($pos < $limit) {
            $eol = strpos($raw, "\n", $pos + 52);
            if ($eol === false) break;
            $slug = substr($raw, $pos + self::URI_OFFSET, $eol - $pos - 51);
            if (!isset($slugs[$slug])) {
                $slugs[$slug] = true;
                $noNew = 0;
            } elseif (++$noNew > 1900) {
                break;
            }
            $pos = $eol + 1;
        }

        return array_keys($slugs);
    }

    private static function parseRange($fh, $start, $end, $slugMap, $dateIds, &$counts)
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
            $fence = $lastNl - self::LOOP_FENCE;

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
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p);
                if ($comma === false) break;
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 4, 7)]]++;
                $p = $comma + 52;
            }
        }
    }

    private static function generateJson($out, $counts, $slugs, $dates)
    {
        $fp = fopen($out, 'wb');
        stream_set_write_buffer($fp, 1_048_576);
        fwrite($fp, '{');

        $dCount = count($dates);

        $datePrefixes = [];
        for ($d = 0; $d < $dCount; $d++) {
            $datePrefixes[$d] = "        \"20{$dates[$d]}\": ";
        }

        $sep  = "\n    ";
        $base = 0;

        foreach ($slugs as $slug) {
            $firstDate = -1;
            for ($d = 0; $d < $dCount; $d++) {
                if ($counts[$base + $d] !== 0) {
                    $firstDate = $d;
                    break;
                }
            }

            if ($firstDate === -1) {
                $base += $dCount;
                continue;
            }

            $buf = $sep . "\"\\/blog\\/$slug\": {\n" . $datePrefixes[$firstDate] . $counts[$base + $firstDate];
            $sep = ",\n    ";

            for ($d = $firstDate + 1; $d < $dCount; $d++) {
                $count = $counts[$base + $d];
                if ($count === 0) continue;
                $buf .= ",\n" . $datePrefixes[$d] . $count;
            }

            $buf .= "\n    }";
            fwrite($fp, $buf);
            $base += $dCount;
        }

        fwrite($fp, "\n}");
        fclose($fp);
    }
}