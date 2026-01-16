# Package Structure and Setup for Docusaurus Plugins

This guide covers the complete package structure, dependencies, and configuration for publishing Docusaurus plugins.

## Standard Directory Structure

### Remark/Rehype Plugin

```
docusaurus-plugin-name/
├── src/
│   └── index.js              # Main plugin implementation
├── test/ or __tests__/
│   ├── fixtures/             # Test fixtures
│   └── plugin.test.js        # Tests
├── index.js                  # Re-export from src
├── index.d.ts                # TypeScript type definitions
├── package.json
├── README.md
├── LICENSE
├── .gitignore
├── .npmignore
├── .editorconfig
└── .github/
    └── workflows/
        └── ci.yml            # GitHub Actions CI/CD
```

### Lifecycle/Content Plugin

```
docusaurus-plugin-name/
├── src/
│   ├── index.js              # Main plugin file
│   ├── clientModule.js       # Browser-side code (optional)
│   ├── types.ts              # TypeScript types
│   └── utils/                # Utility functions
│       ├── parser.js
│       └── validators.js
├── theme/                    # Theme components (optional)
│   ├── Component.js
│   └── Component.css
├── test/
│   └── index.test.js
├── index.js                  # Entry point
├── index.d.ts                # Type definitions
├── package.json
├── tsconfig.json             # TypeScript config (if using TS)
├── jest.config.js            # Jest config
├── README.md
├── LICENSE
└── CHANGELOG.md
```

## package.json Configuration

### Minimal package.json (Remark Plugin)

```json
{
  "name": "docusaurus-plugin-glossary",
  "version": "1.0.0",
  "description": "Docusaurus remark plugin for glossary tooltips",
  "main": "index.js",
  "types": "index.d.ts",

  "keywords": ["docusaurus", "remark", "plugin", "glossary", "documentation"],

  "author": "Your Name <email@example.com>",
  "license": "MIT",

  "repository": {
    "type": "git",
    "url": "https://github.com/username/docusaurus-plugin-glossary"
  },

  "bugs": {
    "url": "https://github.com/username/docusaurus-plugin-glossary/issues"
  },

  "homepage": "https://github.com/username/docusaurus-plugin-glossary#readme",

  "files": ["index.js", "index.d.ts", "lib/", "src/"],

  "dependencies": {
    "unist-util-visit": "^4.0.0"
  },

  "peerDependencies": {
    "@docusaurus/core": "^3.0.0",
    "remark": "^13.0.0 || ^14.0.0"
  },

  "devDependencies": {
    "@types/node": "^18.0.0",
    "@types/unist": "^2.0.0",
    "jest": "^29.0.0",
    "remark": "^14.0.0"
  },

  "scripts": {
    "test": "jest",
    "test:watch": "jest --watch",
    "lint": "eslint .",
    "format": "prettier --write \"**/*.{js,json,md}\""
  },

  "engines": {
    "node": ">=18.0.0"
  }
}
```

### Complete package.json (Lifecycle Plugin)

```json
{
  "name": "@org/docusaurus-plugin-changelog",
  "version": "2.1.0",
  "description": "Changelog content plugin for Docusaurus",
  "main": "lib/index.js",
  "types": "lib/index.d.ts",

  "keywords": ["docusaurus", "plugin", "content", "changelog", "release-notes"],

  "author": "Organization Name",
  "license": "MIT",

  "repository": {
    "type": "git",
    "url": "https://github.com/org/docusaurus-plugin-changelog",
    "directory": "packages/plugin-changelog"
  },

  "publishConfig": {
    "access": "public"
  },

  "files": ["lib/", "theme/", "index.js", "index.d.ts"],

  "dependencies": {
    "fs-extra": "^11.0.0",
    "gray-matter": "^4.0.3",
    "js-yaml": "^4.1.0",
    "rss": "^1.2.2"
  },

  "peerDependencies": {
    "@docusaurus/core": "^3.0.0",
    "react": "^18.0.0",
    "react-dom": "^18.0.0"
  },

  "devDependencies": {
    "@docusaurus/core": "^3.0.0",
    "@docusaurus/types": "^3.0.0",
    "@types/jest": "^29.0.0",
    "@types/node": "^18.0.0",
    "@types/react": "^18.0.0",
    "@typescript-eslint/eslint-plugin": "^6.0.0",
    "@typescript-eslint/parser": "^6.0.0",
    "eslint": "^8.0.0",
    "eslint-config-prettier": "^9.0.0",
    "jest": "^29.0.0",
    "prettier": "^3.0.0",
    "react": "^18.0.0",
    "react-dom": "^18.0.0",
    "typescript": "^5.0.0"
  },

  "scripts": {
    "build": "tsc",
    "build:watch": "tsc --watch",
    "test": "jest",
    "test:watch": "jest --watch",
    "lint": "eslint src --ext .ts,.tsx",
    "lint:fix": "eslint src --ext .ts,.tsx --fix",
    "format": "prettier --write \"src/**/*.{ts,tsx,js,json}\"",
    "typecheck": "tsc --noEmit",
    "prepublishOnly": "npm run build && npm test"
  },

  "engines": {
    "node": ">=18.0.0"
  }
}
```

## TypeScript Configuration

### tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "lib": ["ES2020", "DOM"],
    "jsx": "react",

    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "./lib",
    "rootDir": "./src",

    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,

    "moduleResolution": "node",
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,

    "resolveJsonModule": true,
    "allowSyntheticDefaultImports": true
  },

  "include": ["src/**/*"],
  "exclude": ["node_modules", "lib", "test", "**/*.test.ts"]
}
```

## TypeScript Type Definitions

### Remark Plugin Types

```typescript
// index.d.ts
import { Plugin } from 'unified';
import { Root } from 'mdast';

/**
 * Options for the glossary plugin
 */
export interface RemarkGlossaryOptions {
  /**
   * Directory containing term definitions
   * @default './docs/terms'
   */
  termsDir?: string;

  /**
   * Root documentation directory
   * @default './docs'
   */
  docsDir?: string;

  /**
   * Output path for generated glossary
   * @default './docs/glossary.md'
   */
  glossaryFilepath?: string;

  /**
   * Custom component path for tooltips
   */
  componentPath?: string;
}

/**
 * Remark plugin for adding glossary tooltips to documentation
 */
declare const remarkGlossary: Plugin<[RemarkGlossaryOptions?], Root>;

export default remarkGlossary;
```

### Lifecycle Plugin Types

```typescript
// index.d.ts
import { Plugin, LoadContext, OptionValidationContext } from '@docusaurus/types';

/**
 * Changelog entry data structure
 */
export interface ChangelogEntry {
  id: string;
  slug: string;
  title: string;
  version: string;
  date: string;
  type: 'feature' | 'fix' | 'breaking' | 'improvement';
  body: string;
  filePath: string;
}

/**
 * Plugin options
 */
export interface PluginOptions {
  /**
   * Path to changelog directory
   * @default 'changelog'
   */
  changelogPath?: string;

  /**
   * Base URL path for changelog routes
   * @default 'changelog'
   */
  routeBasePath?: string;

  /**
   * Glob patterns for files to include
   * @default ['**\/*.md']
   */
  include?: string[];

  /**
   * Generate RSS feed
   * @default true
   */
  generateRss?: boolean;
}

/**
 * Plugin content loaded in browser
 */
export interface PluginContent {
  entries: ChangelogEntry[];
  latestVersion: string;
}

declare const plugin: (context: LoadContext, options: PluginOptions) => Plugin<ChangelogEntry[]>;

export default plugin;

/**
 * Validate plugin options
 */
export function validateOptions(
  context: OptionValidationContext<PluginOptions, PluginOptions>
): PluginOptions;
```

## Jest Configuration

### jest.config.js

```javascript
module.exports = {
  testEnvironment: 'node',
  testMatch: ['**/__tests__/**/*.js', '**/*.test.js'],
  collectCoverageFrom: ['src/**/*.js', '!src/**/*.test.js', '!**/node_modules/**'],
  coverageThreshold: {
    global: {
      branches: 80,
      functions: 80,
      lines: 80,
      statements: 80,
    },
  },
  transform: {
    '^.+\\.jsx?$': 'babel-jest',
  },
};
```

## ESLint Configuration

### .eslintrc.js

```javascript
module.exports = {
  root: true,
  env: {
    node: true,
    es2020: true,
  },
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended', 'prettier'],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2020,
    sourceType: 'module',
  },
  rules: {
    '@typescript-eslint/no-explicit-any': 'warn',
    '@typescript-eslint/explicit-module-boundary-types': 'off',
    'no-console': 'warn',
  },
};
```

## Prettier Configuration

### .prettierrc

```json
{
  "semi": true,
  "trailingComma": "es5",
  "singleQuote": true,
  "printWidth": 100,
  "tabWidth": 2,
  "useTabs": false
}
```

## .npmignore

```
# Source files
src/
test/
__tests__/

# Config files
.github/
.vscode/
*.config.js
tsconfig.json
.eslintrc.js
.prettierrc

# Documentation
*.md
!README.md
!LICENSE

# Misc
coverage/
.DS_Store
*.log
```

## .gitignore

```
# Dependencies
node_modules/

# Build output
lib/
dist/
*.tsbuildinfo

# Testing
coverage/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
npm-debug.log*

# Environment
.env
.env.local
```

## GitHub Actions CI/CD

### .github/workflows/ci.yml

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest

    strategy:
      matrix:
        node-version: [18.x, 20.x]

    steps:
      - uses: actions/checkout@v3

      - name: Use Node.js ${{ matrix.node-version }}
        uses: actions/setup-node@v3
        with:
          node-version: ${{ matrix.node-version }}
          cache: 'npm'

      - name: Install dependencies
        run: npm ci

      - name: Run linter
        run: npm run lint

      - name: Run tests
        run: npm test -- --coverage

      - name: Type check
        run: npm run typecheck

      - name: Build
        run: npm run build

      - name: Upload coverage
        uses: codecov/codecov-action@v3
        with:
          file: ./coverage/coverage-final.json

  publish:
    needs: test
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Setup Node.js
        uses: actions/setup-node@v3
        with:
          node-version: '18.x'
          registry-url: 'https://registry.npmjs.org'

      - name: Install dependencies
        run: npm ci

      - name: Build
        run: npm run build

      - name: Publish to npm
        run: npm publish
        env:
          NODE_AUTH_TOKEN: ${{ secrets.NPM_TOKEN }}
```

## README.md Template

````markdown
# docusaurus-plugin-name

> Brief description of what the plugin does

## Installation

```bash
npm install docusaurus-plugin-name
# or
yarn add docusaurus-plugin-name
```
````

## Usage

Add to your `docusaurus.config.js`:

```javascript
module.exports = {
  plugins: [
    [
      'docusaurus-plugin-name',
      {
        // Options
        option1: 'value1',
        option2: true,
      },
    ],
  ],
};
```

## Options

| Option    | Type      | Default     | Description |
| --------- | --------- | ----------- | ----------- |
| `option1` | `string`  | `'default'` | Description |
| `option2` | `boolean` | `false`     | Description |

## Examples

### Basic Example

```javascript
// Example code
```

### Advanced Example

```javascript
// Advanced usage
```

## API

### Plugin Methods

#### `loadContent()`

Description...

#### `contentLoaded({ content, actions })`

Description...

## License

MIT

````

## Versioning and Publishing

### Semantic Versioning

- **MAJOR** (1.0.0 → 2.0.0): Breaking changes
- **MINOR** (1.0.0 → 1.1.0): New features (backward compatible)
- **PATCH** (1.0.0 → 1.0.1): Bug fixes

### Publishing to npm

```bash
# 1. Login to npm
npm login

# 2. Test build
npm run build
npm test

# 3. Update version
npm version patch  # or minor, major

# 4. Publish
npm publish

# For scoped packages
npm publish --access public
````

## Best Practices

1. **Use TypeScript** - Better DX, fewer bugs
2. **Write tests** - Aim for 80%+ coverage
3. **Document thoroughly** - README, JSDoc, examples
4. **Semantic versioning** - Follow SemVer strictly
5. **CI/CD** - Automate testing and publishing
6. **Peer dependencies** - Mark Docusaurus as peer dep
7. **Keep minimal** - Only include necessary files in package
8. **Changelog** - Maintain CHANGELOG.md
