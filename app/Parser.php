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
    private const int CHUNK_SIZE  = 2_097_152;
    private const int READ_BUFFER = 163_840;
    private const int URI_OFFSET  = 25;

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

        $boundaries   = $this->calculateSplits($input);
        $counts       = array_fill(0, $slugCount * $dateCount, 0);
        $boundaryCount = count($boundaries) - 1;

        $fh = fopen($input, 'rb');
        stream_set_read_buffer($fh, 0);
        for ($c = 0; $c < $boundaryCount; $c++) {
            $this->parseRange($fh, $boundaries[$c], $boundaries[$c + 1], $slugMap, $dateIds, $counts);
        }
        fclose($fh);

        $this->generateJson($output, $counts, $slugs, $dateList);
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
                    $map[$date]  = $id;
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
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;

                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;
            }

            while ($p < $lastNl) {
                $comma = strpos($buffer, ',', $p);
                $counts[$slugMap[substr($buffer, $p, $comma - $p)] + $dateIds[substr($buffer, $comma + 3, 8)]]++;
                $p = $comma + 52;
            }
        }
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