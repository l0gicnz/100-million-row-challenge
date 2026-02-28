<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const WORKERS = 8;
    private const BUFFER_SIZE = 65536;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '4G');

        $canFork = function_exists('pcntl_fork');
        if (!$canFork) {
            $this->singleProcessFallback($inputPath, $outputPath);
            return;
        }

        $fileSize = filesize($inputPath);
        $numWorkers = self::WORKERS;

        $starts = [0];
        $fp = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($fp, (int) ($w * $fileSize / $numWorkers));
            fgets($fp);
            $starts[$w] = ftell($fp);
        }
        $starts[$numWorkers] = $fileSize;
        fclose($fp);

        $sockets = [];
        $pids = [];

        for ($w = 0; $w < $numWorkers; $w++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);

            $pid = pcntl_fork();
            if ($pid === 0) {
                fclose($pair[0]);
                $this->processChunkSocket($inputPath, $starts[$w], $starts[$w + 1], $pair[1]);
                fclose($pair[1]);
                exit(0);
            }
            fclose($pair[1]);
            $sockets[] = $pair[0];
            $pids[] = $pid;
        }

        $merged = [];
        foreach ($sockets as $socket) {
            $data = '';
            while (!feof($socket)) {
                $data .= fread($socket, 65536);
            }
            fclose($socket);

            $chunk = igbinary_unserialize($data);
            if (is_array($chunk)) {
                foreach ($chunk as $path => $dates) {
                    if (!isset($merged[$path])) $merged[$path] = [];
                    foreach ($dates as $date => $count) {
                        $merged[$path][$date] = ($merged[$path][$date] ?? 0) + $count;
                    }
                }
            }
            unset($chunk);
        }

        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        // Sort each path's dates ascending
        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $json = json_encode($merged, JSON_PRETTY_PRINT);

        file_put_contents($outputPath, $json);
    }

    private function processChunkSocket(string $filePath, int $start, int $end, $socket): void
    {
        $fp = fopen($filePath, 'rb');
        fseek($fp, $start);

        $results = [];
        $current = $start;

        while ($current < $end && ($line = fgets($fp)) !== false) {
            $current += strlen($line);

            $parts = explode(',', $line, 2);
            if (count($parts) < 2) continue;
            [$url, $date] = $parts;
            $date = substr($date, 0, 10);

            $path = $this->normalizePath($url);

            $results[$path][$date] = ($results[$path][$date] ?? 0) + 1;
        }
        fclose($fp);

        // Sort dates in this chunk ascending
        foreach ($results as &$dates) {
            ksort($dates);
        }
        unset($dates);

        fwrite($socket, igbinary_serialize($results));
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

        // Sort each path's dates ascending
        foreach ($merged as &$dates) {
            ksort($dates);
        }
        unset($dates);

        $json = json_encode($merged, JSON_PRETTY_PRINT);
        file_put_contents($outputPath, $json);
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