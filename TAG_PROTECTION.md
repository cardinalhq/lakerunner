# Tag Protection Policy

This document describes how LakeRunner enforces controlled releases through GitHub tag protection rules.

## Protected Tag Patterns

### Protected (Cannot be manually created)
- `v*.*.*` - Production release tags (e.g., `v1.2.3`, `v2.0.1`)
- Only the "Promote Release" GitHub Action can create these tags

### Allowed (Can be manually created)
- `v*.*.*-rc*` - Release candidates (e.g., `v1.2.3-rc1`, `v1.2.3-rc2`)
- `v*.*.*-beta*` - Beta releases (e.g., `v1.2.3-beta1`)
- `v*.*.*-alpha*` - Alpha releases (e.g., `v1.2.3-alpha1`)
- `v*.*.*-hotfix*` - Emergency hotfix tags (e.g., `v1.2.3-hotfix1`)

## Setup Instructions

### 1. Configure Tag Protection Rules

In GitHub repository settings:

1. Navigate to **Settings** → **General** → **Tag protection rules**
2. Click **Add rule**
3. Configure the rule:
   ```
   Tag name pattern: v[0-9]+.[0-9]+.[0-9]+
   [x] Restrict pushes that create matching tags
   ```

This prevents anyone (including admins) from manually creating `v1.2.3` style tags.

### 2. Create Privileged Token

The promotion workflow needs a token that can bypass tag protection:

#### Option A: GitHub App (Recommended)
1. Create a GitHub App with permissions:
   - **Repository permissions:**
     - Contents: Write
     - Metadata: Read
     - Actions: Write
2. Install the app on your repository
3. Add app credentials to repository secrets

#### Option B: Fine-Grained Personal Access Token
1. Create a fine-grained PAT with:
   - **Resource owner:** Your organization
   - **Repository access:** Selected repositories (lakerunner)
   - **Permissions:**
     - Contents: Write
     - Metadata: Read
2. Add token as `RELEASE_TOKEN` in repository secrets

### 3. Repository Secrets

Add these secrets to **Settings** → **Secrets and variables** → **Actions**:

```
RELEASE_TOKEN: <your-privileged-token>
```

## How It Works

### Blocked Actions
```bash
# These will be rejected by GitHub:
git tag v1.2.3
git push origin v1.2.3

# GitHub UI release creation for v1.2.3 will also be blocked
```

### Allowed Actions
```bash
# Pre-release tags can still be created manually:
git tag v1.2.3-rc1
git push origin v1.2.3-rc1

# The promotion workflow can create production tags:
# (Uses privileged RELEASE_TOKEN that bypasses protection)
```

## Release Workflow

### 1. Create Release Candidate
```bash
git tag v1.2.3-rc1
git push origin v1.2.3-rc1
```
- Allowed - triggers RC build workflow
- Creates: `public.ecr.aws/cardinalhq.io/lakerunner:v1.2.3-rc1`

### 2. Test Release Candidate
- Deploy RC to staging environment
- Run integration tests
- Customer validation
- Security scanning

### 3. Promote to Production
- Use **"Promote Release"** GitHub Action workflow
- Input: RC version → Final version
- Creates protected tags: `v1.2.3`, `v1.2`, `v1`, `stable`
- Only automation can do this

## Benefits

### Security
- Prevents accidental production releases
- Ensures all production images are tested RCs
- Maintains audit trail through GitHub Actions

### Process Enforcement
- Forces use of RC → production promotion workflow
- Prevents bypassing testing phases
- Standardizes release process across team

### Accountability
- All production releases have GitHub Actions logs
- Clear promotion history in workflow runs
- Traceable from RC to production

## Emergency Procedures

### Hotfix Process
For critical production issues:

1. **Create hotfix RC:**
   ```bash
   git tag v1.2.4-hotfix1-rc1
   git push origin v1.2.4-hotfix1-rc1
   ```

2. **Test minimally but thoroughly**

3. **Promote using workflow:**
   - RC: `v1.2.4-hotfix1-rc1`
   - Final: `v1.2.4`

### Breaking Tag Protection (Emergency Only)
If absolutely necessary, repository admins can:
1. Temporarily disable tag protection rule
2. Create manual tag
3. Re-enable protection immediately
4. Document incident in post-mortem

## Troubleshooting

### "Remote rejected" Error
```
! [remote rejected] v1.2.3 -> v1.2.3 (deny updating a ref with this name)
```
**Solution:** Use RC tags (`v1.2.3-rc1`) and promotion workflow instead.

### Promotion Workflow Fails
**Check:**
- `RELEASE_TOKEN` secret is configured
- Token has correct permissions
- RC image exists in registry

### GitHub UI Release Blocked
**Solution:** Create releases only after promotion workflow completes:
1. Promote RC using workflow
2. Create GitHub release using the created `v1.2.3` tag

This policy ensures that every production release is a thoroughly tested release candidate, maintaining both safety and traceability.