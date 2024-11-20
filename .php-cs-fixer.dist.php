<?php

$finder = (new PhpCsFixer\Finder())
    ->in(__DIR__);

return (new PhpCsFixer\Config())
    ->setRules([
        '@PSR12'       => true,
        'array_syntax' => ['syntax' => 'short'],
        'braces_position' => [
            'classes_opening_brace'  => 'same_line',
            'functions_opening_brace' => 'same_line',
        ],
        'binary_operator_spaces' => [
            'operators' => [
                '=>' => 'align_single_space_minimal',
            ]
        ],
    ])
    ->setFinder($finder);
