<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const WORKERS = 8;            // Match physical cores
    private const CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB read buffer

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '8G');

        if (!function_exists('pcntl_fork')) {
            $this->singleProcessFallback($inputPath, $outputPath);
            return;
        }

        $fileSize = filesize($inputPath);
        $numWorkers = self::WORKERS;

        // Calculate start positions per worker
        $starts = [0];
        $fp = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($fp, (int) ($w * $fileSize / $numWorkers));
            fgets($fp); // move to next line
            $starts[$w] = ftell($fp);
        }
        $starts[$numWorkers] = $fileSize;
        fclose($fp);

        $pids = [];
        $tempFiles = [];

        // Fork workers
        for ($w = 0; $w < $numWorkers; $w++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $this->processChunkFile($inputPath, $starts[$w], $starts[$w + 1], $w);
                exit(0);
            }
            $pids[] = $pid;
            $tempFiles[] = sys_get_temp_dir() . "/parser_worker_$w.tmp";
        }

        // Wait for all workers
        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        // Merge worker temp files
        $merged = [];
        foreach ($tempFiles as $file) {
            $data = unserialize(file_get_contents($file));
            if (!is_array($data)) continue;

            foreach ($data as $path => $dates) {
                if (!isset($merged[$path])) $merged[$path] = [];
                foreach ($dates as $date => $count) {
                    $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                }
            }

            unlink($file);
        }

        // Sort dates per path
        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        // Stream JSON output exactly like original
        $out = fopen($outputPath, 'wb');
        fwrite($out, "{\n");
        $firstPath = true;
        foreach ($merged as $path => $dates) {
            if (!$firstPath) fwrite($out, ",\n");

            $jsonDates = json_encode($dates, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            $lines = explode("\n", $jsonDates);
            foreach ($lines as $i => &$line) {
                if ($i > 0) $line = '    ' . $line;
            }
            $indentedDates = implode("\n", $lines);

            fwrite($out, "    " . json_encode($path) . ": {$indentedDates}");
            $firstPath = false;
        }
        fwrite($out, "\n}");
        fclose($out);
    }

    private function processChunkFile(string $filePath, int $start, int $end, int $workerIndex): void
    {
        $fp = fopen($filePath, 'rb');
        fseek($fp, $start);

        $results = [];
        while (($line = fgets($fp)) !== false) {
            if (ftell($fp) > $end) break;

            $parts = explode(',', $line, 2);
            if (count($parts) < 2) continue;
            [$url, $date] = $parts;
            $date = substr($date, 0, 10);

            $path = $this->normalizePath($url);
            $results[$path][$date] = ($results[$path][$date] ?? 0) + 1;
        }
        fclose($fp);

        // Serialize array to temp file (fast and memory efficient)
        $tempFile = sys_get_temp_dir() . "/parser_worker_$workerIndex.tmp";
        file_put_contents($tempFile, serialize($results));
    }

    private function singleProcessFallback(string $inputPath, string $outputPath): void
    {
        $fp = fopen($inputPath, 'rb');
        $merged = [];
        while (($line = fgets($fp)) !== false) {
            $parts = explode(',', $line, 2);
            if (count($parts) < 2) continue;
            [$url, $date] = $parts;
            $date = substr($date, 0, 10);

            $path = $this->normalizePath($url);
            $merged[$path][$date] = ($merged[$path][$date] ?? 0) + 1;
        }
        fclose($fp);

        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $out = fopen($outputPath, 'wb');
        fwrite($out, "{\n");
        $firstPath = true;
        foreach ($merged as $path => $dates) {
            if (!$firstPath) fwrite($out, ",\n");

            $jsonDates = json_encode($dates, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            $lines = explode("\n", $jsonDates);
            foreach ($lines as $i => &$line) {
                if ($i > 0) $line = '    ' . $line;
            }
            $indentedDates = implode("\n", $lines);

            fwrite($out, "    " . json_encode($path) . ": {$indentedDates}");
            $firstPath = false;
        }
        fwrite($out, "\n}");
        fclose($out);
    }

    private function normalizePath(string $url): string
    {
        $schemePos = strpos($url, '://');
        if ($schemePos !== false) {
            $firstSlash = strpos($url, '/', $schemePos + 3);
            $url = ($firstSlash !== false) ? substr($url, $firstSlash) : '/';
        }

        if (($q = strpos($url, '?')) !== false) $url = substr($url, 0, $q);
        if (($h = strpos($url, '#')) !== false) $url = substr($url, 0, $h);

        return $url === '' ? '/' : $url;
    }
}