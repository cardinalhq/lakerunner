# Docusaurus Configuration - Detailed Guide

## Configuration File Structure

Docusaurus configuration can be in multiple formats:

### TypeScript (Recommended)

```typescript
import { Config } from '@docusaurus/types';
import { themes as prismThemes } from 'prism-react-renderer';

const config: Config = {
  // Configuration here
};

export default config;
```

### JavaScript (ESM)

```javascript
export default {
  // Configuration here
};
```

### JavaScript (CommonJS)

```javascript
module.exports = {
  // Configuration here
};
```

### Async Configuration

```typescript
export default async function createConfig(): Promise<Config> {
  // Import ESM-only packages
  const mdxMermaid = await import('mdx-mermaid');

  return {
    // Configuration here
  };
}
```

## Required Fields

### title

- Type: `string`
- Required: Yes
- Description: Site title for metadata and browser tabs

### url

- Type: `string`
- Required: Yes
- Format: Must not end with trailing slash
- Example: `'https://example.com'`
- Description: Full URL where site will be hosted

### baseUrl

- Type: `string`
- Required: Yes
- Format: Must start and end with `/`
- Example: `'/'` or `'/docs/'`
- Description: Path where site is served from

## Common Optional Fields

### Site Metadata

- `tagline`: Short description of your site
- `favicon`: Path to favicon (relative to static folder)
- `organizationName`: GitHub org/user name (for deployment)
- `projectName`: GitHub repo name (for deployment)

### Deployment

- `deploymentBranch`: Branch for deployment (default: 'gh-pages')
- `trailingSlash`: Boolean or undefined for trailing slash handling

### Internationalization

```typescript
i18n: {
  defaultLocale: 'en',
  locales: ['en', 'fr', 'es'],
}
```

## Themes Configuration

### Using Presets (Recommended)

```typescript
presets: [
  [
    '@docusaurus/preset-classic',
    {
      docs: {
        sidebarPath: './sidebars.ts',
        editUrl: 'https://github.com/user/repo/tree/main/',
      },
      blog: {
        showReadingTime: true,
      },
      theme: {
        customCss: './src/css/custom.css',
      },
    },
  ],
],
```

### Direct Theme Configuration

```typescript
themes: ['@docusaurus/theme-classic'],
themeConfig: {
  navbar: {
    title: 'My Site',
    logo: {
      alt: 'My Site Logo',
      src: 'img/logo.svg',
    },
    items: [
      {to: '/docs/intro', label: 'Docs', position: 'left'},
    ],
  },
  footer: {
    style: 'dark',
    links: [],
    copyright: `Copyright © ${new Date().getFullYear()}`,
  },
},
```

## Plugins

### Adding Plugins

String format (no options):

```typescript
plugins: ['@docusaurus/plugin-debug'],
```

Array format (with options):

```typescript
plugins: [
  [
    '@docusaurus/plugin-content-docs',
    {
      id: 'community',
      path: 'community',
      routeBasePath: 'community',
    },
  ],
],
```

### Shorthand Notation

Official Docusaurus packages can use shorthand:

- `'classic'` → `'@docusaurus/preset-classic'`
- `'plugin-debug'` → `'@docusaurus/plugin-debug'`

## Custom Fields

Unknown fields cause validation errors. Use `customFields` for custom data:

```typescript
customFields: {
  apiKey: process.env.API_KEY,
  customValue: 'my-value',
  complexData: {
    nested: true,
  },
}
```

Access in components:

```typescript
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

function MyComponent() {
  const {siteConfig} = useDocusaurusContext();
  const apiKey = siteConfig.customFields.apiKey;

  return <div>{apiKey}</div>;
}
```

## Validation Checklist

When modifying config, verify:

1. **Required fields present**:
   - ✅ `title` exists
   - ✅ `url` exists and has no trailing slash
   - ✅ `baseUrl` exists and starts/ends with `/`

2. **Plugins and themes**:
   - ✅ Use proper package names or shorthand
   - ✅ Options passed as second array element
   - ✅ No duplicate plugins

3. **Custom data**:
   - ✅ Unknown fields in `customFields` object
   - ✅ No direct custom properties at root level

4. **File format**:
   - ✅ Valid JS/TS syntax
   - ✅ Proper export (ESM or CommonJS)
   - ✅ TypeScript types imported if using TS

5. **Testing**:
   - ✅ Restart dev server after changes
   - ✅ Build succeeds: `npm run build`
   - ✅ No console errors

## Common Patterns

### Multi-Instance Docs

```typescript
plugins: [
  [
    '@docusaurus/plugin-content-docs',
    {
      id: 'product',
      path: 'product',
      routeBasePath: 'product',
    },
  ],
  [
    '@docusaurus/plugin-content-docs',
    {
      id: 'community',
      path: 'community',
      routeBasePath: 'community',
    },
  ],
],
```

### Environment Variables

```typescript
const config: Config = {
  url: process.env.SITE_URL || 'https://localhost:3000',
  customFields: {
    apiEndpoint: process.env.API_ENDPOINT,
  },
};
```

### Babel Customization

Create `babel.config.js`:

```javascript
module.exports = {
  presets: [require.resolve('@docusaurus/babel/preset')],
  plugins: [
    // Your custom Babel plugins
  ],
};
```

## Troubleshooting

### Common Errors

**"url must not have a trailing slash"**

- Fix: Change `url: 'https://example.com/'` to `url: 'https://example.com'`

**"baseUrl must start and end with /"**

- Fix: Change `baseUrl: 'docs'` to `baseUrl: '/docs/'`

**"Unknown field 'myField'"**

- Fix: Move to `customFields: { myField: 'value' }`

**"Cannot find module '@docusaurus/types'"**

- Fix: Run `npm install --save-dev @docusaurus/types`

### Best Practices

1. Use TypeScript for better autocomplete and type checking
2. Always read existing config before modifying
3. Preserve file format (don't convert ESM to CommonJS)
4. Test locally before deploying
5. Use environment variables for deployment-specific values
6. Document custom fields with comments
7. Keep config organized with clear sections

## Additional Resources

- [Official Configuration API](https://docusaurus.io/docs/api/docusaurus-config)
- [Plugin Configuration](https://docusaurus.io/docs/using-plugins)
- [Theme Configuration](https://docusaurus.io/docs/using-themes)
- [Deployment Guide](https://docusaurus.io/docs/deployment)
