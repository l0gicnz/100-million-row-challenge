<?php

namespace App;

use function array_fill;
use function array_values;
use function chr;
use function chunk_split;
use function fclose;
use function feof;
use function fopen;
use function fread;
use function fseek;
use function fwrite;
use function sodium_add;
use function stream_select;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function stream_socket_pair;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;
use const SEEK_END;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int DISC_READ   = 1_048_576;
    private const int WORKERS     = 9;

    public static function parse($inputPath, $outputPath)
    {
        gc_disable();

        $slugs = [
            'which-editor-to-choose',
            'tackling_responsive_images-part_1',
            'tackling_responsive_images-part_2',
            'image_optimizers',
            'static_sites_vs_caching',
            'stitcher-alpha-4',
            'simplest-plugin-support',
            'stitcher-alpha-5',
            'php-generics-and-why-we-need-them',
            'stitcher-beta-1',
            'array-objects-with-fixed-types',
            'performance-101-building-the-better-web',
            'process-forks',
            'object-oriented-generators',
            'responsive-images-as-css-background',
            'a-programmers-cognitive-load',
            'mastering-key-bindings',
            'stitcher-beta-2',
            'phpstorm-performance',
            'optimised-uuids-in-mysql',
            'asynchronous-php',
            'mysql-import-json-binary-character-set',
            'where-a-curly-bracket-belongs',
            'mysql-query-logging',
            'mysql-show-foreign-key-errors',
            'responsive-images-done-right',
            'phpstorm-tips-for-power-users',
            'what-php-can-be',
            'phpstorm-performance-issues-on-osx',
            'dependency-injection-for-beginners',
            'liskov-and-type-safety',
            'acquisition-by-giants',
            'visual-perception-of-code',
            'service-locator-anti-pattern',
            'the-web-in-2045',
            'eloquent-mysql-views',
            'laravel-view-models',
            'laravel-view-models-vs-view-composers',
            'organise-by-domain',
            'array-merge-vs + ',
            'share-a-blog-assertchris-io',
            'phpstorm-performance-october-2018',
            'structuring-unstructured-data',
            'share-a-blog-codingwriter-com',
            'new-in-php-73',
            'share-a-blog-betterwebtype-com',
            'have-you-thought-about-casing',
            'comparing-dates',
            'share-a-blog-sebastiandedeyne-com',
            'analytics-for-developers',
            'announcing-aggregate',
            'php-jit',
            'craftsmen-know-their-tools',
            'laravel-queueable-actions',
            'php-73-upgrade-mac',
            'array-destructuring-with-list-in-php',
            'unsafe-sql-functions-in-laravel',
            'starting-a-newsletter',
            'short-closures-in-php',
            'solid-interfaces-and-final-rant-with-brent',
            'php-in-2019',
            'starting-a-podcast',
            'a-project-at-spatie',
            'what-are-objects-anyway-rant-with-brent',
            'tests-and-types',
            'typed-properties-in-php-74',
            'preloading-in-php-74',
            'things-dependency-injection-is-not-about',
            'a-letter-to-the-php-team',
            'a-letter-to-the-php-team-reply-to-joe',
            'guest-posts',
            'can-i-translate-your-blog',
            'laravel-has-many-through',
            'laravel-custom-relation-classes',
            'new-in-php-74',
            'php-74-upgrade-mac',
            'php-preload-benchmarks',
            'php-in-2020',
            'enums-without-enums',
            'bitwise-booleans-in-php',
            'event-driven-php',
            'minor-versions-breaking-changes',
            'combining-event-sourcing-and-stateful-systems',
            'array-chunk-in-php',
            'php-8-in-8-code-blocks',
            'builders-and-architects-two-types-of-programmers',
            'the-ikea-effect',
            'php-74-in-7-code-blocks',
            'improvements-on-laravel-nova',
            'type-system-in-php-survey',
            'merging-multidimensional-arrays-in-php',
            'what-is-array-plus-in-php',
            'type-system-in-php-survey-results',
            'constructor-promotion-in-php-8',
            'abstract-resources-in-laravel-nova',
            'braille-and-the-history-of-software',
            'jit-in-real-life-web-applications',
            'php-8-match-or-switch',
            'why-we-need-named-params-in-php',
            'shorthand-comparisons-in-php',
            'php-8-before-and-after',
            'php-8-named-arguments',
            'my-journey-into-event-sourcing',
            'differences',
            'annotations',
            'dont-get-stuck',
            'attributes-in-php-8',
            'the-case-for-transpiled-generics',
            'phpstorm-scopes',
            'why-light-themes-are-better-according-to-science',
            'what-a-good-pr-looks-like',
            'front-line-php',
            'php-8-jit-setup',
            'php-8-nullsafe-operator',
            'new-in-php-8',
            'php-8-upgrade-mac',
            'when-i-lost-a-few-hundred-leads',
            'websites-like-star-wars',
            'php-reimagined',
            'a-storm-in-a-glass-of-water',
            'php-enums-before-php-81',
            'php-enums',
            'dont-write-your-own-framework',
            'honesty',
            'thoughts-on-event-sourcing',
            'what-event-sourcing-is-not-about',
            'fibers-with-a-grain-of-salt',
            'php-in-2021',
            'parallel-php',
            'why-we-need-multi-line-short-closures-in-php',
            'a-new-major-version-of-laravel-event-sourcing',
            'what-about-config-builders',
            'opinion-driven-design',
            'php-version-stats-july-2021',
            'what-about-request-classes',
            'cloning-readonly-properties-in-php-81',
            'an-event-driven-mindset',
            'php-81-before-and-after',
            'optimistic-or-realistic-estimates',
            'we-dont-need-runtime-type-checks',
            'the-road-to-php',
            'why-do-i-write',
            'rational-thinking',
            'named-arguments-and-variadic-functions',
            're-on-using-psr-abstractions',
            'my-ikea-clock',
            'php-81-readonly-properties',
            'birth-and-death-of-a-framework',
            'php-81-new-in-initializers',
            'route-attributes',
            'generics-in-php-video',
            'php-81-in-8-code-blocks',
            'new-in-php-81',
            'php-81-performance-in-real-life',
            'php-81-upgrade-mac',
            'how-to-be-right-on-the-internet',
            'php-version-stats-january-2022',
            'php-in-2022',
            'how-i-plan',
            'twitter-home-made-me-miserable',
            'its-your-fault',
            'dealing-with-dependencies',
            'php-in-2021-video',
            'generics-in-php-1',
            'generics-in-php-2',
            'generics-in-php-3',
            'generics-in-php-4',
            'goodbye',
            'strategies',
            'dealing-with-deprecations',
            'attribute-usage-in-top-php-packages',
            'php-enum-style-guide',
            'clean-and-minimalistic-phpstorm',
            'stitcher-turns-5',
            'php-version-stats-july-2022',
            'evolution-of-a-php-object',
            'uncertainty-doubt-and-static-analysis',
            'road-to-php-82',
            'php-performance-across-versions',
            'light-colour-schemes-are-better',
            'deprecated-dynamic-properties-in-php-82',
            'php-reimagined-part-2',
            'thoughts-on-asymmetric-visibility',
            'uses',
            'php-82-in-8-code-blocks',
            'readonly-classes-in-php-82',
            'deprecating-spatie-dto',
            'php-82-upgrade-mac',
            'php-annotated',
            'you-cannot-find-me-on-mastodon',
            'new-in-php-82',
            'all-i-want-for-christmas',
            'upgrading-to-php-82',
            'php-version-stats-january-2023',
            'php-in-2023',
            'tabs-are-better',
            'sponsors',
            'why-curly-brackets-go-on-new-lines',
            'my-10-favourite-php-functions',
            'acronyms',
            'code-folding',
            'light-colour-schemes',
            'slashdash',
            'thank-you-kinsta',
            'cloning-readonly-properties-in-php-83',
            'limited-by-committee',
            'things-considered-harmful',
            'procedurally-generated-game-in-php',
            'dont-be-clever',
            'override-in-php-83',
            'php-version-stats-july-2023',
            'is-a-or-acts-as',
            'rfc-vote',
            'new-in-php-83',
            'i-dont-know',
            'passion-projects',
            'php-version-stats-january-2024',
            'the-framework-that-gets-out-of-your-way',
            'a-syntax-highlighter-that-doesnt-suck',
            'building-a-custom-language-in-tempest-highlight',
            'testing-patterns',
            'php-in-2024',
            'tagged-singletons',
            'twitter-exit',
            'a-vocal-minority',
            'php-version-stats-july-2024',
            'you-should',
            'new-with-parentheses-php-84',
            'html-5-in-php-84',
            'array-find-in-php-84',
            'its-all-just-text',
            'improved-lazy-loading',
            'i-dont-code-the-way-i-used-to',
            'php-84-at-least',
            'extends-vs-implements',
            'a-simple-approach-to-static-generation',
            'building-a-framework',
            'tagging-tempest-livestream',
            'things-i-learned-writing-a-fiction-novel',
            'unfair-advantage',
            'new-in-php-84',
            'php-version-stats-january-2025',
            'theoretical-engineers',
            'static-websites-with-tempest',
            'request-objects-in-tempest',
            'php-verse-2025',
            'tempest-discovery-explained',
            'php-version-stats-june-2025',
            'pipe-operator-in-php-85',
            'a-year-of-property-hooks',
            'readonly-or-private-set',
            'things-i-wish-i-knew',
            'impact-charts',
            'whats-your-motivator',
            'vendor-locked',
            'reducing-code-motion',
            'sponsoring-open-source',
            'my-wishlist-for-php-in-2026',
            'game-changing-editions',
            'new-in-php-85',
            'flooded-rss',
            'php-2026',
            'open-source-strategies',
            'not-optional',
            'processing-11-million-rows',
            'ai-induced-skepticism',
            'php-86-partial-function-application',
            '11-million-rows-in-seconds',
        ];

        $dateIds = [];
        $dates = [];
        $di = 0;
        for ($y = 21; $y <= 26; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $maxD = match ($m) { 2 => $y === 24 ? 29 : 28, 4, 6, 9, 11 => 30, default => 31 };
                $mStr = ($m < 10 ? '0' : '') . $m;
                $ymStr = ($y % 10) . '-' . $mStr . '-';
                for ($d = 1; $d <= $maxD; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $dateIds[$ymStr . $dStr] = $di;
                    $dates[$di] = '20' . $y . '-' . $mStr . '-' . $dStr;
                    $di++;
                }
            }
        }

        $paths = [];
        $slugBaseMap = [];
        $slugTotal = 0;
        foreach ($slugs as $slug) {
            $paths[$slugTotal] = $slug;
            $slugBaseMap[$slug] = $slugTotal * $di;
            $slugTotal++;
        }
        $outputSize = $slugTotal * $di;

        $bh = fopen($inputPath, 'rb');

        fseek($bh, 0, SEEK_END);
        $fileSize = ftell($bh);
        $step = (int)($fileSize / self::WORKERS);
        $boundaries = [0];
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($bh, $step * $i);
            fgets($bh);
            $boundaries[] = ftell($bh);
        }
        fclose($bh);
        $boundaries[] = $fileSize;

        $sockets = [];
        $next = [];
        for ($i = 0; $i < 256; $i++) { $next[chr($i)] = chr(($i + 1) & 0xFF); }

        $w = self::WORKERS;
        while ($w-- > 0) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            if (pcntl_fork() === 0) {
                fclose($pair[0]);
                $output = str_repeat("\0", $outputSize);
                $handle = fopen($inputPath, 'rb');
                stream_set_read_buffer($handle, 0);
                fseek($handle, $boundaries[$w]);
                $remaining = $boundaries[$w + 1] - $boundaries[$w];

                while ($remaining > 0) {
                    $chunk = fread($handle, $remaining > 262144 ? 262144 : $remaining);
                    $chunkLen = strlen($chunk);
                    $remaining -= $chunkLen;
                    $lastNl = strrpos($chunk, "\n");
                    if ($lastNl === false) break;
                    $tail = $chunkLen - $lastNl - 1;
                    if ($tail > 0) { fseek($handle, -$tail, SEEK_CUR); $remaining += $tail; }

                    $p = 25;
                    while ($p < $lastNl) {
                        $sep = strpos($chunk, ',', $p);
                        if ($sep === false || $sep >= $lastNl) break;
                        $idx = $slugBaseMap[substr($chunk, $p, $sep - $p)] + $dateIds[substr($chunk, $sep + 4, 7)];
                        $output[$idx] = $next[$output[$idx]];
                        $p = $sep + 52;
                    }
                }
                fclose($handle);

                $offset = 0;
                while ($offset < $outputSize) {
                    $written = fwrite($pair[1], substr($output, $offset));
                    if ($written === false || $written === 0) break;
                    $offset += $written;
                }
                exit(0);
            }
            fclose($pair[1]);
            stream_set_blocking($pair[0], false);
            $sockets[$w] = $pair[0];
        }

        $buffers = array_fill(0, self::WORKERS, '');
        $merged = str_repeat("\0", $outputSize * 2);

        while ($sockets !== []) {
            $read = $sockets;
            $write = $except = null;
            if (stream_select($read, $write, $except, 1) > 0) {
                foreach ($read as $id => $s) {
                    $data = fread($s, 65536);
                    if ($data !== '' && $data !== false) {
                        $buffers[$id] .= $data;
                    }
                    if (feof($s)) {
                        if (strlen($buffers[$id]) === $outputSize) {
                            $expanded = chunk_split($buffers[$id], 1, "\0");
                            sodium_add($merged, $expanded);
                        }
                        fclose($s);
                        unset($sockets[$id]);
                    }
                }
            }
        }

        $counts = array_values(unpack('v*', $merged));
        self::writeJson($outputPath, $counts, $paths, $dates, $di, $slugTotal);
    }

    private static function writeJson($outputPath, $counts, $paths, $dates, $dateCount, $slugCount) {
        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 2_097_152);
        $buf = "{\n";
        for ($p = 0; $p < $slugCount; $p++) {
            $slugData = [];
            $base = $p * $dateCount;
            for ($d = 0; $d < $dateCount; $d++) {
                if ($counts[$base + $d] > 0) {
                    $slugData[] = '        "' . $dates[$d] . '": ' . $counts[$base + $d];
                }
            }
            if ($slugData !== []) {
                if ($buf !== "{\n") $buf .= ",\n";
                $buf .= '    "\/blog\/' . $paths[$p] . '": {' . "\n" . implode(",\n", $slugData) . "\n    }";
                if (strlen($buf) > 1048576) { fwrite($out, $buf); $buf = ''; }
            }
        }
        fwrite($out, $buf . "\n}");
        fclose($out);
    }
}