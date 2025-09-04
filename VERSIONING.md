# Lakerunner Versioning Policy

## Semantic Versioning Scheme

Lakerunner follows [Semantic Versioning 2.0.0](https://semver.org/) with the format `vMAJOR.MINOR.PATCH`:

### Version Components

- **MAJOR** (`v1.0.0` → `v2.0.0`): Breaking changes
  - API incompatibilities
  - Configuration format changes
  - Database schema breaking changes requiring manual migration
  - Removal of deprecated features
  - Changes requiring manual intervention during upgrade

- **MINOR** (`v1.0.0` → `v1.1.0`): New features and enhancements
  - New functionality that is backward compatible
  - Database migrations (handled automatically)
  - New configuration options with sensible defaults
  - Performance improvements
  - Features that may introduce moderate risk for users not needing them

- **PATCH** (`v1.0.0` → `v1.0.1`): Bug fixes and safe improvements
  - Bug fixes that don't change behavior
  - Security patches
  - Documentation updates
  - Minor performance optimizations
  - Schema changes treated as code (safe to auto-apply)
  - **Safe to upgrade from any patch version within the same minor version**

### Pre-release Extensions

- **Release Candidates**: `v1.2.0-rc1`, `v1.2.0-rc2`
  - Feature-complete versions ready for testing
  - Used for customer preview and internal validation
  - May have known issues documented in release notes

- **Beta Releases**: `v1.2.0-beta1`
  - Feature-complete but may have undiscovered issues
  - Used for broader testing before RC

- **Development Builds**: `v1.2.0-dev`
  - Continuous integration builds
  - Not recommended for production use
  - May be unstable or incomplete

## Docker Image Tagging Strategy

### Production Tags
- `v1.2.3` - Exact version (recommended for production)
- `v1.2` - Latest patch in minor version (automatic patch updates)
- `v1` - Latest minor in major version (automatic minor updates)
- `stable` - Current production-recommended version

### Development Tags
- `v1.2.3-rc1` - Release candidate
- `v1.2.3-beta1` - Beta release
- `dev-YYYYMMDD-HHMMSS-shortsha` - Development builds with timestamp

### Deprecated Tags
- `latest` - **REMOVED** (too risky for production)
- `latest-dev` - **REMOVED** (replaced with timestamped dev builds)

## Release Process

### 1. Version Planning
- Determine version bump type based on changes
- Update CHANGELOG.md with planned changes
- Create release branch if doing a minor/major release

### 2. Pre-release Testing
- Tag release candidate: `git tag v1.2.0-rc1`
- Deploy to staging environment
- Run integration tests and customer validation

### 3. Production Release
- Tag final version: `git tag v1.2.0`
- GoReleaser automatically builds and pushes images
- Update Helm charts to reference new version
- Update `stable` tag to point to new version

### 4. Patch Releases
- Cherry-pick fixes to release branch
- Tag patch version: `git tag v1.2.1`
- Automatic deployment for patch versions (if configured)

## Database Migration Policy

### Minor Versions
- Database migrations are included and run automatically
- Migrations must be backward compatible within the minor version
- Rolling upgrades supported within minor version family

### Major Versions
- May include breaking schema changes
- Requires manual migration planning
- Downgrade may not be supported
- Coordinate with operations team before deployment

## Upgrade Safety Guidelines

### Safe Upgrades (Automatic)
- **Patch versions**: `v1.2.1` → `v1.2.5` (safe)
- **Minor versions**: `v1.2.x` → `v1.3.0` (safe with migration)

### Planned Upgrades (Manual)
- **Major versions**: `v1.x.x` → `v2.0.0` (breaking changes - requires planning)
- **Cross-minor jumps**: `v1.2.x` → `v1.5.0` (multiple migrations - requires planning)

### Version Selection for Helm Charts
```yaml
# Recommended: Pin to exact version for production
image:
  tag: "v1.2.3"

# Alternative: Auto-patch updates within minor version
image:
  tag: "v1.2"

# For development environments only
image:
  tag: "dev-20250817-143022-a1b2c3d"
```

## Implementation Notes

- All version tags must be prefixed with `v` (e.g., `v1.0.0`, not `1.0.0`)
- GoReleaser automatically creates multi-architecture manifests
- Development builds include timestamp and git commit for traceability
- The `stable` tag is manually updated after production validation

## Breaking Change Policy

When introducing breaking changes:
1. **Deprecation Notice**: Mark features as deprecated in minor release
2. **Migration Guide**: Provide clear upgrade documentation
3. **Grace Period**: Minimum one minor version before removal
4. **Major Release**: Remove deprecated features in next major version

This policy ensures predictable, safe deployments while maintaining the flexibility to innovate and fix issues rapidly.