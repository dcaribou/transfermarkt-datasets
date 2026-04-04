---
name: process-issue
description: >
  Two-step workflow for resolving GitHub issues: first create a plan, then execute it.
  Use this skill whenever the user wants to work on a GitHub issue, fix a bug from the
  issue tracker, or plan changes for an issue number. Triggers on phrases like
  "process issue", "work on issue #N", "plan issue", "fix issue #N", or any reference
  to implementing a GitHub issue by number.
---

# Process GitHub Issue

A two-step workflow for resolving GitHub issues: first create a plan, then execute it.

## Usage

```
/process-issue plan <issue_number>    — analyze issue and create a plan document
/process-issue execute <issue_number> — execute the existing plan and close the issue
```

Parse the arguments to extract `step` (must be `plan` or `execute`) and `issue_number` (positive integer). If arguments are missing or invalid, print the usage above and stop.

---

## Step 1: `plan`

### 1.1 Create a feature branch

Before any work, create and switch to a feature branch:

```sh
git checkout -b fix/<issue_number>
```

If the branch already exists, switch to it (`git checkout fix/<issue_number>`).

### 1.2 Fetch the issue

Run:

```sh
gh issue view <issue_number> --json title,body,labels,comments,state
```

This uses the current repo's remote automatically — no need to specify `--repo`.

If the issue does not exist, report the error and stop.
If the issue is already closed, warn the user and stop.

### 1.3 Analyze the codebase

Based on the issue title, body, labels, and comments:

- Identify which files are relevant (dbt models, raw data sources, scripts, config files)
- Read those files to understand current behavior
- If the issue references data problems, query the data using DuckDB to confirm:
  ```sh
  .venv/bin/python -c "import duckdb; print(duckdb.connect('dbt/duck.db').sql('YOUR QUERY').fetchdf().to_markdown(index=False))"
  ```
- Understand the root cause thoroughly before proposing changes

### 1.4 Determine fix location

After analyzing the codebase, determine whether the fix belongs in **this repo** (dbt models, scripts, config) or in the **upstream scraper** (`dcaribou/transfermarkt-scraper`).

The fix belongs in the scraper when:

- The root cause is missing or incorrect data in the raw JSON that the scraper produces
- A simpler/cleaner fix would be to extend the scraper's extraction logic rather than work around it in dbt
- The issue explicitly mentions scraper behavior or raw data fields that don't exist yet

If the fix belongs in this repo, continue to **1.5 Create the plan document**.

If the fix belongs in the scraper, follow **1.4a Upstream fix workflow** instead.

### 1.4a Upstream fix workflow (scraper-side fixes)

When the fix requires changes in the scraper:

**1. Create an issue on the scraper repo**

```sh
gh issue create --repo dcaribou/transfermarkt-scraper \
  --title "<concise description of the scraper change needed>" \
  --body "$(cat <<'EOF'
## Context

Downstream issue: dcaribou/transfermarkt-datasets#<issue_number>

<Explain what the scraper should change: which asset/page is affected, what data is missing or incorrect, and what the scraper should extract instead.>

## Expected outcome

<Describe the expected shape of the raw data after the fix — new fields, corrected values, etc.>
EOF
)"
```

Capture the new issue URL from the command output.

**2. Label the datasets issue as blocked**

```sh
gh label create "blocked:upstream" --description "Blocked by a required change in an upstream dependency" --color FBCA04 --force
gh issue edit <issue_number> --add-label "blocked:upstream"
```

**3. Comment on the datasets issue**

```sh
gh issue comment <issue_number> --body "This issue requires an upstream change in the scraper. Created <scraper_issue_url> to track the required work. Marking as blocked until the scraper change is available."
```

**4. Create a plan document recording the decision**

Write a plan document at `docs/plans/<issue_number>-<slug>.md` with this structure:

```markdown
# Fix #<issue_number>: <issue title>

## Context

<What is the problem and why it requires a scraper-side fix rather than a dbt workaround.>

## Upstream issue

<scraper_issue_url>

## What needs to happen

1. The scraper change described in the upstream issue is implemented and released
2. Raw data is re-acquired with the updated scraper
3. Any necessary dbt model changes are made in this repo to consume the new/changed fields
```

Do NOT commit the plan automatically.

**5. Report to the user**

Print:
- The scraper issue URL
- The blocked label applied to the datasets issue
- The plan document path
- Remind the user that once the scraper fix is available, they can update the plan and run `/process-issue execute <issue_number>`

Then **stop** — do not continue to step 1.5.

### 1.5 Create the plan document

Generate a slug from the issue title: lowercase, replace spaces and special characters with hyphens, truncate to ~50 characters. Write the plan to:

```
docs/plans/<issue_number>-<slug>.md
```

The plan document should follow this structure:

```markdown
# Fix #<issue_number>: <issue title or concise description>

## Context

<What is the problem? Why does it happen? Include data evidence if applicable.>

## Changes

### 1. `<file path>`

<Describe the specific change. Include code snippets with before/after where helpful.>

### 2. `<file path>`

<Next change...>

## Verification

<Full commands to run to verify the fix, including any dbt builds, queries, or tests.
Include a clear expected result for each command so it's obvious whether it passed.
For simple changes where dbt build success is sufficient, that's fine as the only check.
For data fixes, include a query that confirms the data is correct.>
```

The Verification section is the heart of the plan — it defines "done". Each verification step should be a concrete command with an expected result. Keep it proportional to the complexity of the change: a one-model fix might just need `dbt build -s model_name --target dev` to succeed, while a data quality fix should include a query proving the data is correct.

### 1.6 Present the plan for review

Print the plan file path and a summary of the proposed changes. Tell the user to review the plan, edit it if needed, and run `/process-issue execute <issue_number>` when ready.

Do NOT commit the plan automatically — the user may want to review or edit it first. If the user asks you to commit it, then stage and commit only the plan document:

```sh
git add docs/plans/<issue_number>-<slug>.md
git commit -m "Add plan for #<issue_number>: <short description>"
```

Do NOT make any code changes during the plan step.
Do NOT push to remote.

---

## Step 2: `execute`

### 2.1 Find the plan

Look for a file matching `docs/plans/<issue_number>-*.md`.

If no plan file exists, tell the user to run `/process-issue plan <issue_number>` first and stop.

### 2.2 Switch to the feature branch

Make sure you're on the `fix/<issue_number>` branch. If not, switch to it. If it doesn't exist, create it.

### 2.3 Read and parse the plan

Read the plan document. Extract:

- The list of file changes from the **Changes** section
- The verification commands from the **Verification** section

### 2.4 Execute the changes

Implement exactly the changes described in the plan.

- Do not add changes that are not in the plan.
- Do not skip changes that are in the plan.
- If you encounter an obstacle not anticipated by the plan, **stop and explain the problem** rather than improvising.
- If the plan references a file that does not exist, stop and report the issue.

### 2.5 Run verification

Run the verification commands from the plan. Check whether the expected results are met.

- **If verification passes:** proceed to commit.
- **If verification fails:** diagnose the problem. Attempt to fix only if the fix is clearly within the scope of the plan. If the fix is out of scope, report the failure (expected vs. actual) and stop without committing.

### 2.6 Commit

Remove the plan document (it has served its purpose) and commit all changes:

```sh
git rm docs/plans/<issue_number>-*.md
git add <all changed files>
git commit -m "Fix #<issue_number>: <short description>"
```

The commit message starts with `Fix #<issue_number>:` so that GitHub auto-closes the issue when the branch is merged.

### 2.7 Report

Print a summary: which files were changed, the verification results, the commit hash, and the branch name.

Remind the user they can push the branch and create a PR when ready. Do NOT push to remote or close the issue — that happens when the PR is merged.

---

## Error Handling

| Case | Behavior |
|------|----------|
| Missing or invalid arguments | Print usage and stop |
| Issue does not exist | Report error, stop |
| Issue is already closed | Warn, stop |
| No plan exists for execute step | Tell user to run plan step first, stop |
| Verification fails after changes | Do NOT commit; report expected vs. actual |
| Plan references a nonexistent file | Stop and report |
| Unexpected obstacle during execution | Stop and explain rather than improvising |
| Fix belongs in the upstream scraper | Create scraper issue, label as blocked, stop |

## Environment Notes

- Always use `../.venv/bin/dbt` when running dbt from the `dbt/` directory, or `.venv/bin/dbt` from repo root
- DuckDB queries: `.venv/bin/python -c "import duckdb; ..."`
- Use `--target dev` for dbt builds
- Raw data uses European date format (DD/MM/YY)
- The plan step does deep codebase analysis; the execute step is mechanical and faithful to the plan
- Neither step pushes to the remote repository
