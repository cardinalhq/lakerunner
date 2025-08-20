# LakeRunner Release Process

This document outlines the recommended release process for LakeRunner, following our semantic versioning strategy.

## Release Types

### Patch Release (v1.2.3 → v1.2.4)

**When:** Bug fixes, security patches, safe improvements
**Risk:** Low
**Automation:** Can be fully automated
**Deployment:** Rolling upgrades supported

### Minor Release (v1.2.x → v1.3.0)

**When:** New features, database migrations, backward-compatible changes
**Risk:** Medium
**Automation:** Semi-automated with approval gates
**Deployment:** Requires migration coordination

### Major Release (v1.x.x → v2.0.0)

**When:** Breaking changes, API changes, architecture changes
**Risk:** High
**Automation:** Manual with extensive testing
**Deployment:** Requires migration planning and possible downtime

## Pre-Release Checklist

### 1. Version Planning

- [ ] Review merged PRs since last release
- [ ] Determine version bump type (patch/minor/major)
- [ ] Check for breaking changes or migrations
- [ ] Update CHANGELOG.md with release notes
- [ ] Ensure all tests pass on main branch

### 2. Release Preparation

- [ ] Create release branch (for minor/major): `release/v1.3.0`
- [ ] Update version references in code if needed
- [ ] Run full test suite including integration tests
- [ ] Check database migration scripts for correctness
- [ ] Review security implications of changes

### 3. Release Candidate (for minor/major releases)

- [ ] Tag release candidate: `git tag v1.3.0-rc1`
- [ ] Deploy RC to staging environment
- [ ] Run automated test suite against staging
- [ ] Customer/internal validation testing
- [ ] Load testing for performance impact
- [ ] Security scanning and vulnerability assessment

## Release Execution

### 1. Create Release Tag

```bash
# For patch releases (can be automated)
git checkout main
git pull origin main
git tag v1.2.4
git push origin v1.2.4

# For minor/major releases (from release branch)
git checkout release/v1.3.0
git tag v1.3.0
git push origin v1.3.0
```

### 2. Build and Push Images

GoReleaser automatically triggers on tag push:

- Builds multi-architecture binaries
- Creates Docker images with version tags
- Pushes to ECR registry
- Creates release artifacts

### 3. Update Helm Charts

- [ ] Update image tags in Helm values files
- [ ] Test Helm chart deployment in staging
- [ ] Submit PR for Helm chart updates
- [ ] Merge after validation

### 4. Production Deployment

#### Patch Releases (Automated)

```bash
# Automatic rolling update
helm upgrade lakerunner charts/lakerunner \
  --set image.tag=v1.2.4 \
  --set updateStrategy.type=RollingUpdate
```

#### Minor/Major Releases (Coordinated)

```bash
# 1. Backup database
kubectl exec -it postgres-0 -- pg_dump lrdb > backup-pre-v1.3.0.sql

# 2. Deploy with migration
helm upgrade lakerunner charts/lakerunner \
  --set image.tag=v1.3.0 \
  --set migration.enabled=true \
  --wait --timeout=10m

# 3. Verify deployment
kubectl get pods -l app=lakerunner
kubectl logs -l app=lakerunner --tail=100
```

### 5. Post-Release Tasks

- [ ] Update `stable` tag to point to new release
- [ ] Monitor application metrics and logs
- [ ] Verify database migrations completed successfully
- [ ] Update documentation if needed
- [ ] Announce release to team/customers
- [ ] Monitor for issues in first 24 hours

## Rollback Procedures

### Patch Release Rollback

```bash
# Quick rollback to previous version
helm rollback lakerunner
```

### Minor/Major Release Rollback

```bash
# 1. Stop application
kubectl scale deployment lakerunner --replicas=0

# 2. Restore database backup (if migrations were run)
kubectl exec -it postgres-0 -- psql lrdb < backup-pre-v1.3.0.sql

# 3. Deploy previous version
helm upgrade lakerunner charts/lakerunner \
  --set image.tag=v1.2.3 \
  --wait
```

## Release Automation

### GitHub Actions Integration

```yaml
# Proposed workflow triggers
on:
  push:
    tags:
      - 'v*.*.*'      # Trigger on version tags
      - 'v*.*.*-rc*'  # Trigger on release candidates
```

### Automated Checks

- [ ] All tests must pass
- [ ] Security scans must pass
- [ ] Database migration tests must pass
- [ ] Performance regression tests must pass
- [ ] Documentation builds successfully

### Approval Gates

- **Patch releases:** Automatic after CI passes
- **Minor releases:** Requires team lead approval
- **Major releases:** Requires architecture review and customer notification

## Release Communication

### Internal Communication

- Update team Slack channel with release status
- Send email for major releases with downtime
- Update internal documentation and runbooks

### Customer Communication

- **Patch:** Automatic notification via status page
- **Minor:** Email notification with feature highlights
- **Major:** Coordinated announcement with migration guide

## Monitoring and Validation

### Post-Release Monitoring (24 hours)

- [ ] Application error rates < baseline
- [ ] Response times within acceptable ranges
- [ ] Database performance metrics stable
- [ ] No increase in support tickets
- [ ] All integration tests passing

### Success Criteria

- Zero-downtime deployment (patch/minor)
- All services healthy and responsive
- Database migrations completed without errors
- No critical bugs reported in first 24 hours

## Emergency Procedures

### Hotfix Process (Critical Issues)

1. Create hotfix branch from release tag
2. Apply minimal fix for critical issue
3. Tag hotfix version (e.g., v1.2.4-hotfix1)
4. Fast-track through testing
5. Deploy with expedited approval

### Incident Response

- Immediate rollback if critical service impact
- Coordinate with on-call team for after-hours releases
- Have database backup and rollback procedures ready
- Maintain communication channels during deployment

## Tools and Dependencies

- **Git:** Version control and tagging
- **GoReleaser:** Binary and image building
- **Helm:** Kubernetes deployment management
- **kubectl:** Kubernetes cluster management
- **AWS CLI/crane:** Image registry management
- **GitHub Actions:** CI/CD automation

This process balances automation for low-risk changes with human oversight for higher-risk releases, ensuring both velocity and safety.
