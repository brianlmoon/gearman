# Repository Guidelines

## Project Structure & Module Organization
- Runtime source lives in `src/` under the `Moonspot\Gearman` namespace (e.g., `src/Client.php`, `src/Worker.php`).
- Historic PEAR-style code remains under `Net/` for reference but is not part of the Composer autoload tree.
- Tests live in `tests/`: PHPUnit suites (e.g., `tests/Net/Gearman/TaskTest.php`) plus PHPT fixtures (`tests/001-echo_req.phpt`, etc.).
- Composer metadata (`composer.json`) defines dependencies and PSR-4 autoloading; run tooling from the repo root.

## Build, Test, and Development Commands
- `composer install` – install PHP dependencies; required before any other command.
- `vendor/bin/phpunit -c phpunit.xml.dist` – run the primary PHPUnit suite (functional tests excluded).
- `vendor/bin/phpunit -c phpunit.xml.functional-dist` – execute functional/integration tests; ensure `NET_GEARMAN_TEST_SERVER` points to a live Gearman daemon (Docker example: `docker run --rm -d -p 4730:4730 artefactual/gearmand:latest`).
- `php -l $(find src -name '*.php')` – quick syntax check across runtime classes.

## Coding Style & Naming Conventions
- Target PHP 7.4+/8.x with typed properties, nullable hints, and snake_case for properties/variables; methods stay camelCase.
- Namespaces follow `Moonspot\Gearman`; prefer protected visibility for members and avoid private unless mandated.
- Keep PHPDoc blocks informative with concrete param/return details, especially when types are mixed.

## Testing Guidelines
- PHPUnit backs unit coverage; PHPT files validate protocol-level behavior.
- Name tests descriptively (`testSend`, `testTaskStatus`) and store new fixtures under `tests/`.
- Functional suites require a running Gearman server; update `phpunit.xml.functional-dist` if the host/port differs from localhost:4730.

## Commit & Pull Request Guidelines
- Use concise, present-tense commit messages (e.g., “Add typed properties to Client”).
- PRs should summarize scope, note testing performed, and link issues when applicable; attach logs/screenshots for behavior changes.
