<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const WORKERS = 4;
    private const BUFFER_SIZE = 8192;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '1G');
        
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
            
            $chunk = unserialize($data);
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

        $json = json_encode($merged, JSON_PRETTY_PRINT);
        $json = str_replace("\n", "\r\n", $json);
        
        $fp = fopen($outputPath, 'wb');
        fwrite($fp, $json);
        fclose($fp);
    }

    private function processChunkSocket(string $filePath, int $start, int $end, $socket): void
    {
        $fp = fopen($filePath, 'rb');
        fseek($fp, $start);
        
        $results = [];
        $current = $start;

        while ($current < $end && ($line = fgets($fp)) !== false) {
            $current += strlen($line);
            $comma = strpos($line, ',');
            if ($comma === false) continue;

            $url = substr($line, 0, $comma);
            $date = substr($line, $comma + 1, 10);
            
            $path = $url;
            $schemePos = strpos($path, '://');
            if ($schemePos !== false) {
                $firstSlash = strpos($path, '/', $schemePos + 3);
                $path = ($firstSlash !== false) ? substr($path, $firstSlash) : '/';
            }
            
            if (($q = strpos($path, '?')) !== false) $path = substr($path, 0, $q);
            if (($h = strpos($path, '#')) !== false) $path = substr($path, 0, $h);
            $path = ($path === '' || $path === false) ? '/' : $path;

            $results[$path][$date] = ($results[$path][$date] ?? 0) + 1;
        }
        fclose($fp);

        fwrite($socket, serialize($results));
    }

    private function singleProcessFallback(string $inputPath, string $outputPath): void
    {
        $fp = fopen($inputPath, 'rb');
        $merged = [];
        while (($line = fgets($fp)) !== false) {
            $comma = strpos($line, ',');
            if ($comma === false) continue;
            $url = substr($line, 0, $comma);
            $date = substr($line, $comma + 1, 10);
            $path = $url;
            $merged[$path][$date] = ($merged[$path][$date] ?? 0) + 1;
        }
        fclose($fp);
    }
}