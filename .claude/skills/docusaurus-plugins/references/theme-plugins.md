# Theme Plugins for Docusaurus

Theme plugins provide custom **themes** and allow **component swizzling** to override default Docusaurus components.

## When to Use Theme Plugins

- Create custom themes
- Override default components (Navbar, Footer, etc.)
- Provide reusable component sets
- Brand customization across sites
- Custom layouts

## Theme Plugin Structure

```javascript
// plugins/theme-custom/index.js
const path = require('path');

module.exports = function themePlugin(context, options) {
  return {
    name: 'docusaurus-theme-custom',

    getThemePath() {
      // Return path to theme components
      return path.resolve(__dirname, './theme');
    },

    getTypeScriptThemePath() {
      // Return path to TypeScript theme types
      return path.resolve(__dirname, './theme');
    },

    getClientModules() {
      // Global CSS and client-side code
      return [
        path.resolve(__dirname, './theme/global.css'),
        path.resolve(__dirname, './theme/prism-theme.js'),
      ];
    },
  };
};
```

## Theme Directory Structure

```
plugins/theme-custom/
├── index.js                    # Plugin definition
└── theme/
    ├── global.css              # Global styles
    ├── Layout.js               # Main layout component
    ├── Navbar.js               # Navigation bar
    ├── Footer.js               # Footer
    ├── DocPage.js              # Documentation page layout
    ├── BlogPostPage.js         # Blog post layout
    ├── MDXComponents.js        # Custom MDX components
    └── hooks/
        └── useCustomHook.js    # Custom React hooks
```

## Configuration

```javascript
// docusaurus.config.js
module.exports = {
  themes: [
    './plugins/theme-custom',
    // Or npm package
    '@org/docusaurus-theme-custom',
  ],

  // Or with options
  themes: [
    [
      './plugins/theme-custom',
      {
        customColors: {
          primary: '#007bff',
          secondary: '#6c757d',
        },
      },
    ],
  ],
};
```

## Component Swizzling

### What is Swizzling?

Swizzling allows you to **override** or **wrap** default Docusaurus components.

### Swizzle Commands

```bash
# List all swizzleable components
npm run swizzle @docusaurus/theme-classic -- --list

# Eject a component (full control, breaks with updates)
npm run swizzle @docusaurus/theme-classic Navbar -- --eject

# Wrap a component (safer, preserves original)
npm run swizzle @docusaurus/theme-classic Navbar -- --wrap

# Swizzle TypeScript version
npm run swizzle @docusaurus/theme-classic Navbar -- --typescript
```

### Ejecting vs Wrapping

**Ejecting (--eject)**

- Full control over component
- Copy entire component to your src/theme
- ⚠️ Breaks with Docusaurus updates
- Use for major customizations

**Wrapping (--wrap)**

- Wraps original component
- Preserve default behavior
- ✅ Safer, updates compatible
- Use for minor additions

## Example: Custom Theme Components

### 1. Custom Navbar

```javascript
// theme/Navbar.js
import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import './Navbar.css';

export default function Navbar() {
  const { siteConfig } = useDocusaurusContext();

  return (
    <nav className="custom-navbar">
      <div className="navbar-brand">
        <Link to="/">
          <img src={siteConfig.logo} alt={siteConfig.title} />
          <span>{siteConfig.title}</span>
        </Link>
      </div>

      <div className="navbar-links">
        <Link to="/docs">Docs</Link>
        <Link to="/blog">Blog</Link>
        <Link to="/about">About</Link>
      </div>

      <div className="navbar-actions">
        <button className="theme-toggle">🌓</button>
        <a href={siteConfig.githubUrl} className="github-link">
          GitHub
        </a>
      </div>
    </nav>
  );
}
```

### 2. Custom Footer

```javascript
// theme/Footer.js
import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function Footer() {
  const { siteConfig } = useDocusaurusContext();
  const currentYear = new Date().getFullYear();

  return (
    <footer className="custom-footer">
      <div className="footer-content">
        <div className="footer-section">
          <h4>Documentation</h4>
          <ul>
            <li>
              <Link to="/docs/intro">Getting Started</Link>
            </li>
            <li>
              <Link to="/docs/api">API Reference</Link>
            </li>
            <li>
              <Link to="/docs/guides">Guides</Link>
            </li>
          </ul>
        </div>

        <div className="footer-section">
          <h4>Community</h4>
          <ul>
            <li>
              <a href={siteConfig.discord}>Discord</a>
            </li>
            <li>
              <a href={siteConfig.twitter}>Twitter</a>
            </li>
            <li>
              <a href={siteConfig.github}>GitHub</a>
            </li>
          </ul>
        </div>

        <div className="footer-section">
          <h4>More</h4>
          <ul>
            <li>
              <Link to="/blog">Blog</Link>
            </li>
            <li>
              <a href="/changelog">Changelog</a>
            </li>
            <li>
              <Link to="/privacy">Privacy Policy</Link>
            </li>
          </ul>
        </div>
      </div>

      <div className="footer-bottom">
        <p>
          © {currentYear} {siteConfig.organizationName}. All rights reserved.
        </p>
      </div>
    </footer>
  );
}
```

### 3. Custom MDX Components

```javascript
// theme/MDXComponents.js
import React from 'react';
import MDXComponents from '@theme-original/MDXComponents';
import Highlight from '@site/src/components/Highlight';
import CodeBlock from '@theme/CodeBlock';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

// Custom components available in MDX
export default {
  ...MDXComponents,

  // Override default components
  code: CodeBlock,

  // Add custom components
  Highlight,
  Callout: ({ type = 'info', children }) => (
    <div className={`callout callout-${type}`}>{children}</div>
  ),

  YouTube: ({ id }) => (
    <div className="video-container">
      <iframe
        src={`https://www.youtube.com/embed/${id}`}
        frameBorder="0"
        allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
        allowFullScreen
      />
    </div>
  ),

  Tabs,
  TabItem,
};
```

### 4. Custom Layout

```javascript
// theme/Layout.js
import React from 'react';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Navbar from '@theme/Navbar';
import Footer from '@theme/Footer';
import './Layout.css';

export default function Layout({ children, title, description }) {
  const { siteConfig } = useDocusaurusContext();
  const pageTitle = title ? `${title} | ${siteConfig.title}` : siteConfig.title;

  return (
    <>
      <Head>
        <title>{pageTitle}</title>
        <meta name="description" content={description || siteConfig.tagline} />
        <meta property="og:title" content={pageTitle} />
        <meta property="og:description" content={description || siteConfig.tagline} />
      </Head>

      <div className="layout">
        <Navbar />
        <main className="main-content">{children}</main>
        <Footer />
      </div>
    </>
  );
}
```

### 5. Wrapped Component Example

```javascript
// theme/DocPage.js (wrapped)
import React from 'react';
import DocPage from '@theme-original/DocPage';
import { useLocation } from '@docusaurus/router';

export default function DocPageWrapper(props) {
  const location = useLocation();

  // Add custom behavior before/after original component
  React.useEffect(() => {
    console.log('Doc page viewed:', location.pathname);
  }, [location]);

  return (
    <>
      {/* Custom elements before */}
      <div className="custom-banner">Special announcement!</div>

      {/* Original DocPage component */}
      <DocPage {...props} />

      {/* Custom elements after */}
      <div className="custom-feedback">Was this helpful?</div>
    </>
  );
}
```

## Global Styles

```css
/* theme/global.css */
:root {
  /* Custom CSS variables */
  --ifm-color-primary: #2e8555;
  --ifm-color-primary-dark: #29784c;
  --ifm-color-primary-darker: #277148;
  --ifm-color-primary-darkest: #205d3b;
  --ifm-color-primary-light: #33925d;
  --ifm-color-primary-lighter: #359962;
  --ifm-color-primary-lightest: #3cad6e;

  --ifm-code-font-size: 95%;
  --ifm-font-family-base: 'Inter', system-ui, sans-serif;
}

html[data-theme='dark'] {
  --ifm-color-primary: #25c2a0;
  --ifm-background-color: #1b1b1d;
}

/* Custom styles */
.custom-navbar {
  /* Navbar styles */
}

.custom-footer {
  /* Footer styles */
}
```

## Custom Prism Theme

```javascript
// theme/prism-theme.js
const theme = {
  plain: {
    color: '#F8F8F2',
    backgroundColor: '#282A36',
  },
  styles: [
    {
      types: ['prolog', 'constant', 'builtin'],
      style: {
        color: 'rgb(189, 147, 249)',
      },
    },
    {
      types: ['inserted', 'function'],
      style: {
        color: 'rgb(80, 250, 123)',
      },
    },
    {
      types: ['deleted'],
      style: {
        color: 'rgb(255, 85, 85)',
      },
    },
    {
      types: ['changed'],
      style: {
        color: 'rgb(255, 184, 108)',
      },
    },
    {
      types: ['punctuation', 'symbol'],
      style: {
        color: 'rgb(248, 248, 242)',
      },
    },
    {
      types: ['string', 'char', 'tag', 'selector'],
      style: {
        color: 'rgb(255, 121, 198)',
      },
    },
    {
      types: ['keyword', 'variable'],
      style: {
        color: 'rgb(189, 147, 249)',
        fontStyle: 'italic',
      },
    },
    {
      types: ['comment'],
      style: {
        color: 'rgb(98, 114, 164)',
      },
    },
  ],
};

export default theme;
```

## Package Structure

```json
{
  "name": "@org/docusaurus-theme-custom",
  "version": "1.0.0",
  "main": "index.js",
  "types": "index.d.ts",
  "keywords": ["docusaurus", "theme"],

  "peerDependencies": {
    "@docusaurus/core": "^3.0.0",
    "react": "^18.0.0",
    "react-dom": "^18.0.0"
  },

  "dependencies": {
    "clsx": "^2.0.0"
  }
}
```

## TypeScript Support

```typescript
// index.d.ts
import { Plugin, LoadContext } from '@docusaurus/types';

export interface ThemeOptions {
  customColors?: {
    primary?: string;
    secondary?: string;
  };
}

declare const theme: (context: LoadContext, options: ThemeOptions) => Plugin<void>;

export default theme;
```

## Best Practices

1. **Prefer wrapping over ejecting** - More update-safe
2. **Use CSS variables** - Easy theming and consistency
3. **Keep components modular** - Reusable across pages
4. **Provide TypeScript types** - Better DX
5. **Document swizzled components** - Team awareness
6. **Test responsive design** - Mobile-first approach
7. **Maintain accessibility** - ARIA labels, keyboard navigation

## Common Customizations

### Dark Mode Toggle

```javascript
// theme/ColorModeToggle.js
import React from 'react';
import { useColorMode } from '@docusaurus/theme-common';

export default function ColorModeToggle() {
  const { colorMode, setColorMode } = useColorMode();

  return (
    <button
      onClick={() => setColorMode(colorMode === 'dark' ? 'light' : 'dark')}
      aria-label="Toggle dark mode"
    >
      {colorMode === 'dark' ? '🌞' : '🌙'}
    </button>
  );
}
```

### Search Integration

```javascript
// theme/SearchBar.js
import React from 'react';
import { DocSearch } from '@docsearch/react';

export default function SearchBar() {
  return <DocSearch appId="YOUR_APP_ID" indexName="YOUR_INDEX_NAME" apiKey="YOUR_SEARCH_API_KEY" />;
}
```

### Version Dropdown

```javascript
// theme/NavbarItem/DocsVersionDropdown.js
import React from 'react';
import { useActiveVersion, useVersions } from '@docusaurus/plugin-content-docs/client';

export default function DocsVersionDropdown() {
  const versions = useVersions();
  const activeVersion = useActiveVersion();

  return (
    <select value={activeVersion.name} onChange={(e) => (window.location.href = e.target.value)}>
      {versions.map((version) => (
        <option key={version.name} value={version.path}>
          {version.label}
        </option>
      ))}
    </select>
  );
}
```

## Debugging

Check which components can be swizzled:

```bash
npm run swizzle @docusaurus/theme-classic -- --list
```

Find component source code:

```bash
# Located in node_modules/@docusaurus/theme-classic/src/theme/
ls node_modules/@docusaurus/theme-classic/src/theme/
```
