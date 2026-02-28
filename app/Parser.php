<?php

declare(strict_types=1);

namespace App;

final class Parser
{
    private const WORKERS    = 8;                 // physical cores
    private const CHUNK_SIZE = 32 * 1024 * 1024; // 32 MB per read

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '12G');
        gc_disable();

        if (!function_exists('pcntl_fork')) {
            $this->singleProcessFallback($inputPath, $outputPath);
            return;
        }

        $fileSize = filesize($inputPath);
        $numWorkers = self::WORKERS;

        // Compute worker start positions
        $starts = [0];
        $fp = fopen($inputPath, 'rb');
        for ($w = 1; $w < $numWorkers; $w++) {
            fseek($fp, (int)($w * $fileSize / $numWorkers));
            fgets($fp); // skip partial line
            $starts[$w] = ftell($fp);
        }
        $starts[$numWorkers] = $fileSize;
        fclose($fp);

        // Fork workers
        $pids = [];
        $tempFiles = [];
        for ($w = 0; $w < $numWorkers; $w++) {
            $tmpFile = sys_get_temp_dir() . "/parser_worker_$w.tmp";
            $pid = pcntl_fork();
            if ($pid === -1) throw new \RuntimeException("pcntl_fork failed");
            if ($pid === 0) {
                $this->processChunkDynamic($inputPath, $starts[$w], $starts[$w+1], $tmpFile);
                exit(0);
            }
            $pids[] = $pid;
            $tempFiles[] = $tmpFile;
        }

        // Wait for all workers
        foreach ($pids as $pid) pcntl_waitpid($pid, $status);

        // Merge workers
        $globalPathIds = [];  // path => global ID (in insertion order)
        $allCounts = [];      // globalId => [date => count]

        foreach ($tempFiles as $file) {
            [$localPathIds, $counts] = igbinary_unserialize(file_get_contents($file));
            foreach ($localPathIds as $path => $id) {
                if (!isset($globalPathIds[$path])) {
                    $globalPathIds[$path] = count($globalPathIds);
                    $allCounts[$globalPathIds[$path]] = [];
                }
                $globalId = $globalPathIds[$path];

                foreach ($counts[$id] as $date => $cnt) {
                    $allCounts[$globalId][$date] = ($allCounts[$globalId][$date] ?? 0) + $cnt;
                }
            }
            unlink($file);
        }

        // Sort dates per path only
        foreach ($allCounts as &$dates) ksort($dates);
        unset($dates);

        // Stream JSON output
        $out = fopen($outputPath,'wb');
        fwrite($out,"{\n");
        $first = true;
        foreach ($globalPathIds as $path => $id) {
            if (!$first) fwrite($out,",\n");
            $first = false;

            $jsonDates = json_encode($allCounts[$id], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            $lines = explode("\n", $jsonDates);
            foreach ($lines as $i => &$line) if($i>0)$line='    '.$line;

            fwrite($out,"    ".json_encode($path).": ".implode("\n",$lines));
        }
        fwrite($out,"\n}");
        fclose($out);
    }

    private function processChunkDynamic(string $filePath, int $start, int $end, string $tmpFile): void
    {
        $fp = fopen($filePath, 'rb');
        fseek($fp, $start);

        $buffer = '';
        $remaining = $end - $start;
        $localPathIds = [];
        $counts = [];
        $nextId = 0;

        while ($remaining > 0 && !feof($fp)) {
            $toRead = min(self::CHUNK_SIZE, $remaining);
            $chunk = fread($fp, $toRead);
            if ($chunk === false || $chunk === '') break;
            $remaining -= strlen($chunk);

            $buffer .= $chunk;
            $lines = explode("\n", $buffer);
            $buffer = array_pop($lines); // last line may be partial

            foreach ($lines as $line) {
                $parts = explode(',', $line, 2);
                if (count($parts) < 2) continue;
                [$url, $date] = $parts;
                $date = substr($date,0,10);

                $path = $this->normalizePath($url);
                if (!isset($localPathIds[$path])) {
                    $localPathIds[$path] = $nextId++;
                    $counts[$localPathIds[$path]] = [];
                }
                $pId = $localPathIds[$path];
                $counts[$pId][$date] = ($counts[$pId][$date] ?? 0) + 1;
            }
        }

        fclose($fp);
        file_put_contents($tmpFile, igbinary_serialize([$localPathIds,$counts]));
    }

    private function singleProcessFallback(string $inputPath, string $outputPath): void
    {
        $fp = fopen($inputPath, 'rb');
        $merged = [];
        while (($line = fgets($fp)) !== false) {
            $parts = explode(',', $line, 2);
            if (count($parts) < 2) continue;
            [$url,$date] = $parts;
            $date = substr($date,0,10);
            $path = $this->normalizePath($url);
            $merged[$path][$date] = ($merged[$path][$date] ?? 0) + 1;
        }
        fclose($fp);

        foreach ($merged as &$dates) ksort($dates);
        unset($dates);

        $out = fopen($outputPath,'wb');
        fwrite($out,"{\n");
        $first = true;
        foreach ($merged as $path => $dates) {
            if (!$first) fwrite($out,",\n");
            $first = false;
            $jsonDates = json_encode($dates, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            $lines = explode("\n",$jsonDates);
            foreach ($lines as $i=>&$line) if($i>0)$line='    '.$line;
            fwrite($out,"    ".json_encode($path).": ".implode("\n",$lines));
        }
        fwrite($out,"\n}");
        fclose($out);
    }

    private function normalizePath(string $url): string
    {
        $schemePos = strpos($url,'://');
        if($schemePos!==false){
            $firstSlash = strpos($url,'/',$schemePos+3);
            $url = $firstSlash!==false ? substr($url,$firstSlash) : '/';
        }
        if(($q=strpos($url,'?'))!==false)$url=substr($url,0,$q);
        if(($h=strpos($url,'#'))!==false)$url=substr($url,0,$h);
        return $url==='' ? '/' : $url;
    }
}