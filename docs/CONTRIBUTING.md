# Contributing Guidelines — VoltaGrid Data

> Language: [🇺🇸 English](CONTRIBUTING.md) | [🇪🇸 Español](CONTRIBUTING.es.md)

> **Note (P2):** this guide was adapted from `voltiagrid-api`. Generic workflow (branches, commits, PRs) is final.
> Sections marked `TODO(P2)` need the real Spark/Airflow setup once jobs exist.

This guide covers the full workflow: `git clone` → setup → branch → commit → PR → review → merge.
It combines [Conventional Commits](https://www.conventionalcommits.org/) (like the CoDecide reference repo) with the team's **Jira (KAN)** workflow.

---

## 0. Prerequisites

- Git, Python 3.12, Docker + Docker Compose.
- A GitHub account with access to `VoltiaGrid-Medicion-Inteligente/voltiagrid-data`.
- A Jira account. Your local git email **must match** your Jira email, otherwise Smart Commits won't link:
  ```bash
  git config user.name "Your Name"
  git config user.email "you@jira-email.com"
  ```

## 1. Clone and setup (first time only)

```bash
# HTTPS (simplest)
git clone https://github.com/VoltiaGrid-Medicion-Inteligente/voltiagrid-data.git
cd voltiagrid-data

# or SSH (if you use SSH keys)
# git clone git@github.com:VoltiaGrid-Medicion-Inteligente/voltiagrid-data.git
# cd voltiagrid-data

<!-- TODO(P2): replace with the real local run: venv/Docker, spark-submit --mode dev, one DAG run. -->
python -m pytest tests/ -v
```

Rules:

- **Never commit `.env`.** Only `.env.example` (no secrets) goes to git (CT-02).
- <!-- TODO(P2): document S3/RDS connections via env vars only, no hardcoded endpoints. -->

## 2. Branch Strategy

```
main ────────────── stable branch, PRs merge here (demo / release)
  ├── feature/KAN-12-short-description
  ├── fix/KAN-13-short-description
  ├── refactor/KAN-14-short-description
  ├── docs/KAN-15-short-description
  └── chore/KAN-16-short-description
```

### Branch Naming Convention

```
<type>/KAN-<number>-<short-description>
```

| Type | When to use | Example |
|------|-------------|---------|
| `feature/` | New functionality / user story | `feature/KAN-12-meter-consumer` |
| `fix/` | Bug fix | `fix/KAN-13-login-redirect-loop` |
| `refactor/` | Restructure without behavior change | `refactor/KAN-14-extract-meter-service` |
| `chore/` | Tooling, dependencies, config, CI | `chore/KAN-16-upgrade-pytest` |
| `docs/` | Documentation only | `docs/KAN-15-document-f3-simulator` |
| `test/` | Tests only | `test/KAN-16-seed-hash-test` |

- Jira key in **UPPERCASE** (`KAN-12`, not `kan-12`) so Jira auto-links branch → issue.
- Description in **kebab-case**, short but meaningful, English preferred.
- All branches come from up-to-date `main`.

### Rules

- **Never push directly to `main`.** All changes via Pull Request.
- Any commit pushed directly to `main` will be reverted/deleted.
- One branch per Jira issue/task. If the task grows, split the issue, don't grow the branch.
- Keep `main` green: pull before branching.

Create a branch:

```bash
git checkout main
git pull origin main
git checkout -b feature/KAN-12-meter-consumer
```

## 3. Conventional Commits + Jira

### Format

```
KAN-<number> <type>(<scope>): <description>
```

- The `KAN-XX` prefix keeps Jira automation (branch/commit/PR all linked).
- The rest follows Conventional Commits.

### Types

| Type | When to use |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `refactor` | Neither fix nor feature |
| `style` | Formatting only (no production change) |
| `docs` | Documentation only |
| `chore` | Build, deps, tooling, CI |
| `test` | Add/modify tests |
| `perf` | Performance improvement |

### Scopes (this repo)

Jobs: `jobs`, `clean`, `dedup`, `estimate`, `bands`, `losses`, `events`
Orchestration: `dags`, `airflow`
Cross-cutting: `config`, `docker`, `ci`, `docs`, `deps`, `tests`

### Examples (copy the style)

```
KAN-12 feat(clean): add null and range validation for raw readings
KAN-13 fix(dedup): keep highest sequence on duplicate meter interval
KAN-14 refactor(jobs): extract tariff band assignment helper
KAN-15 docs(dags): document daily close dag schedule
KAN-16 test(losses): cover suspect transformer rule
KAN-15 chore(config): add s3 paths override by env
```

### Rules

- **Jira key first, UPPERCASE** (`KAN-12`, not `kan-12`).
- **Description in English, imperative present tense:** "add" not "added"/"adds".
- **Lowercase description, no trailing period**, concise (<72 chars if possible).
- One logical change per commit. Two unrelated fixes → two commits.
- Small commits preferred. 20+ files in one commit → split it.

Good split:

```
KAN-12 feat(jobs): add tariff band assignment
KAN-12 feat(dedup): wire dedup by sequence into clean job
```

Bad:

```
KAN-12 feat: add lots of stuff   # 35 files, 1200 additions
```

Fix a message before pushing:

```bash
git commit --amend -m "KAN-12 feat(jobs): correct message"
# if already pushed to YOUR branch only:
git push --force-with-lease
```

### Smart Commits (Jira automation)

Only works if `git config user.email` == Jira email:

```
KAN-12 #comment ready for review
KAN-12 #done
```

Use them in a separate commit or in the PR description — don't mix with code changes silently.

## 4. Pull Request Workflow

1. Update from `main`, run checks:

   ```bash
   git checkout feature/KAN-12-meter-consumer
   git pull --rebase origin main
   python -m pytest tests/ -v
   ```

2. Push and open a PR **targeting `main**:

   ```bash
   git push -u origin feature/KAN-12-meter-consumer
   ```

3. PR title = same as commit format (Jira key + Conventional):

   ```
   KAN-12 feat(clean): add null and range validation for raw readings
   ```

4. PR description (required template):

   ```markdown
   ## What
   Brief description of the change.

   ## Why
   Reason + Jira issue (e.g. KAN-12).

   ## How to test
   <!-- TODO(P2): fill the real local run. Skeleton: -->
   1. spark-submit with --mode dev (or Docker equivalent)
   2. python -m pytest tests/ -v

   ## Screenshots / evidence (if applicable)
   ```

5. Wait for review + green CI (lint, tests, secret scan). Address comments with **new commits**, don't rewrite history under review.
6. Maintainer merges into `main` via reviewed PR (squash by default, keeping `KAN-XX` in title). `main` must always stay green and demo-ready.

### Pre-PR checklist

- [ ] Branch from latest `main`, name `type/KAN-XX-kebab-case`.
- [ ] Commits `KAN-XX type(scope): english imperative description`.
- [ ] `pytest` green locally (or in Docker).
- [ ] No secrets/`.env`/credentials in diff (`git status`, `git diff --check`).
- [ ] Reprocessing still idempotent if you touched jobs (re-running changes nothing).
- [ ] PR targets `main`, title + template filled.

## 5. What gets your PR rejected

- Direct push to `main`.
- Missing Jira key or lowercase key (`kan-12`).
- Non-Conventional message (`added stuff`, `fix style.`, capitalised sentence).
- Giant commit, mixed concerns, or untested business rule (every RN-* needs at least one automated test).
- Secrets in code/images, or hardcoded endpoints/credentials.
