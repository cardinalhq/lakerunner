# Lifecycle Plugins for Docusaurus

Lifecycle plugins are the most flexible type of Docusaurus plugin. They implement **lifecycle hooks** that execute at different stages of the build process.

## When to Use Lifecycle Plugins

- Add custom routes and pages
- Modify webpack configuration
- Inject scripts or styles into HTML
- Generate static files during build
- Add global data accessible across the site
- Integrate third-party services
- Custom content processing

## Complete Plugin Structure

```javascript
// plugins/my-plugin/index.js
const path = require('path');

module.exports = function myPlugin(context, options) {
  const { siteConfig, siteDir } = context;
  const { customOption = 'default' } = options;

  return {
    name: 'my-custom-plugin',

    // Load content from files
    async loadContent() {
      // Read files, fetch data, etc.
      const data = await fetchData();
      return data;
    },

    // Make content available globally
    async contentLoaded({ content, actions }) {
      const { setGlobalData, addRoute, createData } = actions;

      // Add global data (accessible via useGlobalData hook)
      setGlobalData({ myData: content });

      // Add custom route
      addRoute({
        path: '/custom-page',
        component: '@site/src/components/CustomPage.js',
        exact: true,
      });

      // Create data file for component props
      const dataPath = await createData('my-data.json', JSON.stringify(content));

      addRoute({
        path: '/data-page',
        component: '@site/src/components/DataPage.js',
        modules: {
          data: dataPath,
        },
        exact: true,
      });
    },

    // Run after build completes
    async postBuild({ outDir, content }) {
      // Generate additional files
      await generateSitemap(outDir);
    },

    // Inject HTML tags
    injectHtmlTags() {
      return {
        headTags: [
          {
            tagName: 'link',
            attributes: {
              rel: 'preconnect',
              href: 'https://fonts.googleapis.com',
            },
          },
          {
            tagName: 'script',
            attributes: {
              src: 'https://analytics.example.com/script.js',
              async: true,
            },
          },
        ],
        preBodyTags: [],
        postBodyTags: [
          {
            tagName: 'script',
            innerHTML: `
              console.log('Page loaded');
            `,
          },
        ],
      };
    },

    // Modify webpack config
    configureWebpack(config, isServer, utils) {
      return {
        resolve: {
          alias: {
            '@components': path.resolve(__dirname, '../../src/components'),
          },
        },
        plugins: [
          // Custom webpack plugins
        ],
      };
    },

    // Provide client modules (run in browser)
    getClientModules() {
      return [path.resolve(__dirname, './clientModule.js')];
    },

    // Get theme path
    getThemePath() {
      return path.resolve(__dirname, './theme');
    },
  };
};
```

## Configuration in docusaurus.config.js

```javascript
// Local plugin
module.exports = {
  plugins: [
    './plugins/my-plugin',
    // Or with options
    [
      './plugins/my-plugin',
      {
        customOption: 'value',
      },
    ],
  ],
};

// npm package
module.exports = {
  plugins: [
    '@org/docusaurus-plugin-name',
    // Or with options
    [
      '@org/docusaurus-plugin-name',
      {
        apiKey: 'xxx',
        enabled: true,
      },
    ],
  ],
};
```

## Lifecycle Hooks

### loadContent()

Load data from files, APIs, or databases.

```javascript
async loadContent() {
  const posts = await fs.readdir('./blog');
  const data = await Promise.all(
    posts.map(async (file) => {
      const content = await fs.readFile(`./blog/${file}`, 'utf-8');
      return parseMarkdown(content);
    })
  );
  return data;
}
```

### contentLoaded({ content, actions })

Process loaded content and create routes/global data.

```javascript
async contentLoaded({ content, actions }) {
  const { setGlobalData, addRoute, createData } = actions;

  // Add global data
  setGlobalData({ posts: content });

  // Create route for each post
  await Promise.all(
    content.map(async (post) => {
      const dataPath = await createData(
        `post-${post.id}.json`,
        JSON.stringify(post)
      );

      addRoute({
        path: `/blog/${post.slug}`,
        component: '@site/src/components/BlogPost.js',
        modules: { post: dataPath },
        exact: true,
      });
    })
  );
}
```

### postBuild({ outDir, content })

Run after build completes. Generate additional files.

```javascript
async postBuild({ outDir, content }) {
  // Generate RSS feed
  const feed = generateRSS(content);
  await fs.writeFile(`${outDir}/rss.xml`, feed);

  // Generate sitemap
  const sitemap = generateSitemap(content);
  await fs.writeFile(`${outDir}/sitemap.xml`, sitemap);
}
```

### injectHtmlTags()

Add scripts, styles, meta tags to HTML.

```javascript
injectHtmlTags({ content }) {
  return {
    headTags: [
      // Analytics
      {
        tagName: 'script',
        attributes: {
          async: true,
          src: 'https://www.googletagmanager.com/gtag/js?id=GA_ID',
        },
      },
      // Meta tags
      {
        tagName: 'meta',
        attributes: {
          name: 'description',
          content: 'Site description',
        },
      },
      // Preconnect
      {
        tagName: 'link',
        attributes: {
          rel: 'preconnect',
          href: 'https://fonts.googleapis.com',
        },
      },
    ],
    postBodyTags: [
      // Inline script
      {
        tagName: 'script',
        innerHTML: `
          window.customConfig = ${JSON.stringify({ key: 'value' })};
        `,
      },
    ],
  };
}
```

### configureWebpack(config, isServer, utils)

Modify webpack configuration.

```javascript
configureWebpack(config, isServer, utils) {
  const { getStyleLoaders } = utils;

  return {
    resolve: {
      alias: {
        '@components': path.resolve(__dirname, '../../src/components'),
      },
    },
    module: {
      rules: [
        {
          test: /\.custom$/,
          use: ['custom-loader'],
        },
      ],
    },
    plugins: [
      new CustomWebpackPlugin(),
    ],
  };
}
```

### getClientModules()

Provide modules that run in the browser.

```javascript
// plugins/my-plugin/index.js
getClientModules() {
  return [path.resolve(__dirname, './analytics.js')];
}

// plugins/my-plugin/analytics.js
export function onRouteUpdate({ location }) {
  // Track page view
  if (typeof window !== 'undefined' && window.gtag) {
    window.gtag('config', 'GA_ID', {
      page_path: location.pathname,
    });
  }
}
```

## Real-World Examples

### 1. Analytics Plugin

```javascript
// plugins/analytics/index.js
module.exports = function analyticsPlugin(context, options) {
  const { trackingId, anonymizeIP = true } = options;

  return {
    name: 'docusaurus-plugin-analytics',

    injectHtmlTags() {
      return {
        headTags: [
          {
            tagName: 'script',
            attributes: {
              async: true,
              src: `https://www.googletagmanager.com/gtag/js?id=${trackingId}`,
            },
          },
          {
            tagName: 'script',
            innerHTML: `
              window.dataLayer = window.dataLayer || [];
              function gtag(){dataLayer.push(arguments);}
              gtag('js', new Date());
              gtag('config', '${trackingId}', {
                anonymize_ip: ${anonymizeIP}
              });
            `,
          },
        ],
      };
    },

    getClientModules() {
      return [path.resolve(__dirname, './trackPageViews.js')];
    },
  };
};

// plugins/analytics/trackPageViews.js
export function onRouteUpdate({ location }) {
  if (typeof window !== 'undefined' && window.gtag) {
    window.gtag('event', 'page_view', {
      page_path: location.pathname,
    });
  }
}
```

### 2. RSS Feed Generator

```javascript
// plugins/rss/index.js
const fs = require('fs-extra');
const RSS = require('rss');

module.exports = function rssPlugin(context, options) {
  const { siteConfig } = context;
  const { feedPath = 'rss.xml', limit = 20 } = options;

  return {
    name: 'docusaurus-plugin-rss',

    async postBuild({ outDir }) {
      // Read blog posts from content
      const posts = await loadBlogPosts();

      // Create RSS feed
      const feed = new RSS({
        title: siteConfig.title,
        description: siteConfig.tagline,
        feed_url: `${siteConfig.url}/${feedPath}`,
        site_url: siteConfig.url,
        language: 'en',
      });

      // Add posts to feed
      posts.slice(0, limit).forEach((post) => {
        feed.item({
          title: post.title,
          description: post.description,
          url: `${siteConfig.url}${post.permalink}`,
          date: post.date,
        });
      });

      // Write RSS file
      await fs.writeFile(path.join(outDir, feedPath), feed.xml({ indent: true }));
    },
  };
};
```

### 3. Dynamic Route Creator

```javascript
// plugins/custom-pages/index.js
const fs = require('fs-extra');
const matter = require('gray-matter');

module.exports = function customPagesPlugin(context, options) {
  const { pagesDir = 'custom-pages' } = options;

  return {
    name: 'docusaurus-plugin-custom-pages',

    async loadContent() {
      const pagesPath = path.join(context.siteDir, pagesDir);
      const files = await fs.readdir(pagesPath);

      const pages = await Promise.all(
        files
          .filter((file) => file.endsWith('.md'))
          .map(async (file) => {
            const content = await fs.readFile(path.join(pagesPath, file), 'utf-8');
            const { data, content: body } = matter(content);

            return {
              id: file.replace('.md', ''),
              ...data,
              body,
            };
          })
      );

      return pages;
    },

    async contentLoaded({ content, actions }) {
      const { addRoute, createData } = actions;

      await Promise.all(
        content.map(async (page) => {
          const dataPath = await createData(`page-${page.id}.json`, JSON.stringify(page));

          addRoute({
            path: `/${page.slug || page.id}`,
            component: '@site/src/components/CustomPage.js',
            modules: { page: dataPath },
            exact: true,
          });
        })
      );
    },
  };
};

// src/components/CustomPage.js
import React from 'react';
import Layout from '@theme/Layout';
import MDXContent from '@theme/MDXContent';

export default function CustomPage({ page }) {
  return (
    <Layout title={page.title}>
      <div className="container margin-vert--lg">
        <h1>{page.title}</h1>
        <MDXContent>{page.body}</MDXContent>
      </div>
    </Layout>
  );
}
```

### 4. Sitemap Generator

```javascript
// plugins/sitemap/index.js
const fs = require('fs-extra');
const { SitemapStream, streamToPromise } = require('sitemap');

module.exports = function sitemapPlugin(context, options) {
  const { siteConfig } = context;
  const { changefreq = 'weekly', priority = 0.7 } = options;

  return {
    name: 'docusaurus-plugin-sitemap',

    async postBuild({ routes, outDir }) {
      const sitemap = new SitemapStream({
        hostname: siteConfig.url,
      });

      // Add routes to sitemap
      routes.forEach((route) => {
        if (!route.path.includes('*') && !route.path.includes(':')) {
          sitemap.write({
            url: route.path,
            changefreq,
            priority,
          });
        }
      });

      sitemap.end();

      // Write sitemap
      const xml = await streamToPromise(sitemap);
      await fs.writeFile(path.join(outDir, 'sitemap.xml'), xml.toString());
    },
  };
};
```

### 5. Environment Variables Plugin

```javascript
// plugins/env-vars/index.js
const webpack = require('webpack');

module.exports = function envVarsPlugin(context, options) {
  const { allowedVars = [] } = options;

  return {
    name: 'docusaurus-plugin-env-vars',

    configureWebpack() {
      const envVars = {};

      allowedVars.forEach((varName) => {
        if (process.env[varName]) {
          envVars[`process.env.${varName}`] = JSON.stringify(process.env[varName]);
        }
      });

      return {
        plugins: [new webpack.DefinePlugin(envVars)],
      };
    },
  };
};

// Usage in docusaurus.config.js
plugins: [
  [
    './plugins/env-vars',
    {
      allowedVars: ['API_URL', 'ANALYTICS_ID'],
    },
  ],
];
```

## Package Structure

```
docusaurus-plugin-name/
├── src/
│   ├── index.js              # Main plugin file
│   ├── clientModule.js       # Browser-side code
│   └── theme/                # Theme components (if any)
│       └── Component.js
├── index.js                  # Re-export from src
├── index.d.ts                # TypeScript definitions
├── package.json
└── README.md
```

## TypeScript Definitions

```typescript
// index.d.ts
import { Plugin, LoadContext, OptionValidationContext } from '@docusaurus/types';

export interface PluginOptions {
  customOption?: string;
  enabled?: boolean;
}

export default function plugin(context: LoadContext, options: PluginOptions): Plugin<any>;

export function validateOptions({
  options,
  validate,
}: OptionValidationContext<PluginOptions, PluginOptions>): PluginOptions;
```

## Option Validation

```javascript
const { Joi } = require('@docusaurus/utils-validation');

function validateOptions({ options, validate }) {
  const schema = Joi.object({
    customOption: Joi.string().default('default'),
    enabled: Joi.boolean().default(true),
    apiKey: Joi.string().required(),
  });

  return validate(schema, options);
}

module.exports = { validateOptions };
```

## Best Practices

1. **Validate options** - Use Joi schemas for type safety
2. **Handle errors gracefully** - Don't crash the build
3. **Document options** - Clear README with examples
4. **Use TypeScript** - Better DX and autocomplete
5. **Cache when possible** - Avoid redundant file reads
6. **Test thoroughly** - Unit and integration tests
7. **Follow naming conventions** - `docusaurus-plugin-*` or `@scope/docusaurus-plugin-*`

## Testing

```javascript
// __tests__/plugin.test.js
const plugin = require('../src/index');

describe('My Plugin', () => {
  const context = {
    siteDir: '/site',
    siteConfig: {
      url: 'https://example.com',
      baseUrl: '/',
    },
  };

  it('returns correct name', () => {
    const instance = plugin(context, {});
    expect(instance.name).toBe('my-plugin');
  });

  it('loads content correctly', async () => {
    const instance = plugin(context, {});
    const content = await instance.loadContent();
    expect(content).toBeDefined();
  });
});
```

## Common Patterns

### Global Data Access

Use `useGlobalData()` hook in React components:

```javascript
import useGlobalData from '@docusaurus/useGlobalData';

function MyComponent() {
  const { myData } = useGlobalData()['my-plugin'];
  return <div>{myData}</div>;
}
```

### Plugin Data Access

Use `usePluginData()` hook:

```javascript
import usePluginData from '@docusaurus/usePluginData';

function MyComponent() {
  const data = usePluginData('my-plugin');
  return <div>{data}</div>;
}
```
