# Migration Integrity Protection

This repository includes automated protection against unintended changes to existing database migration files.

## How It Works

### Local Development
- Run `make check-migration-integrity` to validate migration changes locally
- This is automatically included in `make check`
- The check compares against your base branch (usually `main`)

### GitHub Actions
- The `Migration Integrity Check` workflow runs on all PRs that modify migration files
- The workflow will **block PR merging** if substantive changes are detected in existing migrations
- Uses the same logic as local checks for consistency

### Branch-Aware Logic
The protection system is smart about **which migrations** can be changed:

- **Protected**: Migrations that existed in the base branch (e.g., `main`)
  - Only comment and whitespace changes allowed
- **Freely modifiable**: Migrations created in your current branch  
  - Can be changed, added to, or even restructured during development
- **Never allowed**: Deleting any migration files

## What's Allowed vs. Blocked

### Allowed Changes (for existing migrations)
- Adding comments (SQL `--` style)
- Changing whitespace/indentation
- Adding blank lines
- Reformatting existing code

### Blocked Changes (for existing migrations)
- Changing SQL statements, table names, column names
- Modifying constraints, indexes, or data types  
- Adding/removing enum values
- Deleting migration files
- Any substantive changes to migration logic

## Example: Allowed vs. Blocked

### For Existing Migrations (from base branch)

**Allowed (comment addition to existing migration):**
```diff
-- Create initial tables
+ -- This migration sets up the core database schema
CREATE TABLE users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL
);
```

**Blocked (adding column to existing migration):**
```diff
CREATE TABLE users (
  id UUID PRIMARY KEY,
- name TEXT NOT NULL
+ name TEXT NOT NULL,
+ email TEXT
);
```

### For New Migrations (created in your branch)

**Allowed (any changes during development):**
```diff
CREATE TABLE users (
  id UUID PRIMARY KEY,
- name TEXT NOT NULL
+ name TEXT NOT NULL,
+ email TEXT UNIQUE,
+ created_at TIMESTAMP DEFAULT NOW()
);
```

The system automatically detects which migrations are new vs. existing.

## Setting Up Branch Protection

To make this check **required** for PR merging:

1. Go to your repository **Settings** → **Branches**
2. Add a branch protection rule for `main` (or your default branch)
3. Enable **"Require status checks to pass before merging"**
4. Add **"Check Migration File Integrity"** to required checks
5. Save the protection rule

## When a Check Fails

The GitHub Action will:
1. Mark the PR check as failed
2. Add a detailed comment explaining what went wrong
3. Show exactly what changes were detected
4. Provide instructions on how to fix the issue

## Creating New Migrations

Instead of modifying existing migrations, always create new ones:

```bash
# For lrdb migrations
make new-migration name=add_user_email

# For configdb migrations  
make new-configdb-migration name=add_api_key_permissions
```

## Why This Protection Exists

Database migrations may have already been applied in production environments. Modifying existing migrations can cause:
- Schema inconsistencies between environments
- Failed deployments when migrations don't match applied state
- Data integrity issues
- Rollback complications

This protection ensures migration files remain immutable once committed, maintaining database schema reliability across all environments.