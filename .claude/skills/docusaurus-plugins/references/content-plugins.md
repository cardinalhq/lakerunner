# Content Plugins for Docusaurus

Content plugins create **custom content types** beyond the default docs and blog. They load, process, and make content available throughout your site.

## When to Use Content Plugins

- Custom content types (tutorials, changelog, team members, etc.)
- External data sources (APIs, databases)
- Generated content (API docs from code)
- Multi-source aggregation
- Custom routing patterns

## Content Plugin Structure

```javascript
// plugins/plugin-content-changelog/index.js
const fs = require('fs-extra');
const path = require('path');
const matter = require('gray-matter');

module.exports = function contentChangelogPlugin(context, options) {
  const {
    changelogPath = 'changelog',
    routeBasePath = 'changelog',
    include = ['**/*.md'],
  } = options;

  const contentPath = path.resolve(context.siteDir, changelogPath);

  return {
    name: 'docusaurus-plugin-content-changelog',

    // Load all changelog entries
    async loadContent() {
      const entries = [];
      const files = await fs.readdir(contentPath);

      for (const file of files) {
        if (!file.endsWith('.md')) continue;

        const filePath = path.join(contentPath, file);
        const content = await fs.readFile(filePath, 'utf-8');
        const { data: frontmatter, content: body } = matter(content);

        entries.push({
          id: file.replace('.md', ''),
          slug: frontmatter.slug || file.replace('.md', ''),
          title: frontmatter.title,
          version: frontmatter.version,
          date: frontmatter.date,
          type: frontmatter.type || 'feature', // feature, fix, breaking
          body,
          filePath,
        });
      }

      // Sort by date (newest first)
      entries.sort((a, b) => new Date(b.date) - new Date(a.date));

      return entries;
    },

    // Create routes and make data available
    async contentLoaded({ content, actions }) {
      const { createData, addRoute, setGlobalData } = actions;

      // Set global data (accessible via useGlobalData hook)
      setGlobalData({
        entries: content,
        latestVersion: content[0]?.version,
      });

      // Create changelog list page
      const listDataPath = await createData(
        'changelog-list.json',
        JSON.stringify({ entries: content })
      );

      addRoute({
        path: `/${routeBasePath}`,
        component: '@site/src/components/ChangelogList.js',
        exact: true,
        modules: {
          entries: listDataPath,
        },
      });

      // Create individual entry pages
      await Promise.all(
        content.map(async (entry) => {
          const entryDataPath = await createData(
            `changelog-${entry.id}.json`,
            JSON.stringify(entry)
          );

          addRoute({
            path: `/${routeBasePath}/${entry.slug}`,
            component: '@site/src/components/ChangelogEntry.js',
            exact: true,
            modules: {
              entry: entryDataPath,
            },
          });
        })
      );
    },

    // Optional: Generate additional files after build
    async postBuild({ outDir, content }) {
      // Generate RSS feed
      const rss = generateRSSFeed(content);
      await fs.writeFile(path.join(outDir, 'changelog.xml'), rss);

      // Generate JSON API
      const api = content.map((entry) => ({
        id: entry.id,
        title: entry.title,
        version: entry.version,
        date: entry.date,
        type: entry.type,
      }));
      await fs.writeFile(path.join(outDir, 'changelog.json'), JSON.stringify(api, null, 2));
    },
  };
};
```

## Configuration

```javascript
// docusaurus.config.js
module.exports = {
  plugins: [
    [
      './plugins/plugin-content-changelog',
      {
        changelogPath: 'changelog',
        routeBasePath: 'changelog',
        include: ['**/*.md'],
      },
    ],
  ],
};
```

## Content File Structure

```
changelog/
├── 2024-01-15-v2.0.0.md
├── 2024-01-01-v1.5.0.md
└── 2023-12-15-v1.4.0.md
```

### Example Content File

```markdown
---
slug: v2-0-0
title: Version 2.0.0 Released
version: 2.0.0
date: 2024-01-15
type: breaking
---

# What's New in 2.0.0

Major breaking changes and new features.

## Breaking Changes

- API endpoint restructured
- Configuration format changed

## New Features

- Dark mode support
- Improved performance
```

## Component Examples

### Changelog List Component

```javascript
// src/components/ChangelogList.js
import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import clsx from 'clsx';

export default function ChangelogList({ entries }) {
  return (
    <Layout title="Changelog" description="Product updates and release notes">
      <div className="container margin-vert--lg">
        <h1>Changelog</h1>

        <div className="changelog-list">
          {entries.entries.map((entry) => (
            <div key={entry.id} className="changelog-item">
              <div className="changelog-header">
                <Link to={`/changelog/${entry.slug}`}>
                  <h2>{entry.title}</h2>
                </Link>

                <div className="changelog-meta">
                  <span className={clsx('badge', `badge-${entry.type}`)}>{entry.type}</span>
                  <span className="version">v{entry.version}</span>
                  <time>{new Date(entry.date).toLocaleDateString()}</time>
                </div>
              </div>

              <div className="changelog-preview">{entry.body.slice(0, 200)}...</div>
            </div>
          ))}
        </div>
      </div>
    </Layout>
  );
}
```

### Changelog Entry Component

```javascript
// src/components/ChangelogEntry.js
import React from 'react';
import Layout from '@theme/Layout';
import MDXContent from '@theme/MDXContent';
import Link from '@docusaurus/Link';

export default function ChangelogEntry({ entry }) {
  return (
    <Layout title={entry.title} description={`Release notes for version ${entry.version}`}>
      <div className="container margin-vert--lg">
        <Link to="/changelog" className="back-link">
          ← Back to Changelog
        </Link>

        <article className="changelog-entry">
          <header>
            <h1>{entry.title}</h1>
            <div className="meta">
              <span className="version">Version {entry.version}</span>
              <time>{new Date(entry.date).toLocaleDateString()}</time>
            </div>
          </header>

          <MDXContent>{entry.body}</MDXContent>
        </article>
      </div>
    </Layout>
  );
}
```

## Real-World Examples

### 1. Team Members Plugin

```javascript
// plugins/plugin-content-team/index.js
const fs = require('fs-extra');
const path = require('path');
const yaml = require('js-yaml');

module.exports = function teamPlugin(context, options) {
  const { teamDataPath = 'data/team.yml' } = options;

  return {
    name: 'docusaurus-plugin-content-team',

    async loadContent() {
      const dataPath = path.join(context.siteDir, teamDataPath);
      const data = await fs.readFile(dataPath, 'utf-8');
      const team = yaml.load(data);

      return team.members;
    },

    async contentLoaded({ content, actions }) {
      const { createData, addRoute, setGlobalData } = actions;

      setGlobalData({ members: content });

      // Team list page
      const dataPath = await createData('team.json', JSON.stringify(content));

      addRoute({
        path: '/team',
        component: '@site/src/components/Team.js',
        exact: true,
        modules: { members: dataPath },
      });

      // Individual member pages
      await Promise.all(
        content.map(async (member) => {
          const memberData = await createData(`team-${member.id}.json`, JSON.stringify(member));

          addRoute({
            path: `/team/${member.id}`,
            component: '@site/src/components/TeamMember.js',
            exact: true,
            modules: { member: memberData },
          });
        })
      );
    },
  };
};
```

### 2. External API Content Plugin

```javascript
// plugins/plugin-content-api/index.js
const fetch = require('node-fetch');

module.exports = function apiContentPlugin(context, options) {
  const { apiUrl, apiKey } = options;

  return {
    name: 'docusaurus-plugin-content-api',

    async loadContent() {
      // Fetch data from external API
      const response = await fetch(apiUrl, {
        headers: { Authorization: `Bearer ${apiKey}` },
      });

      const data = await response.json();

      return data.items.map((item) => ({
        id: item.id,
        title: item.title,
        description: item.description,
        category: item.category,
        updatedAt: item.updated_at,
      }));
    },

    async contentLoaded({ content, actions }) {
      const { createData, addRoute, setGlobalData } = actions;

      // Make data globally available
      setGlobalData({ items: content });

      // Create category pages
      const categories = [...new Set(content.map((item) => item.category))];

      await Promise.all(
        categories.map(async (category) => {
          const categoryItems = content.filter((item) => item.category === category);
          const dataPath = await createData(
            `category-${category}.json`,
            JSON.stringify(categoryItems)
          );

          addRoute({
            path: `/items/${category}`,
            component: '@site/src/components/CategoryPage.js',
            exact: true,
            modules: { items: dataPath },
          });
        })
      );
    },
  };
};
```

### 3. Generated API Documentation Plugin

```javascript
// plugins/plugin-api-docs/index.js
const fs = require('fs-extra');
const path = require('path');
const { parseTypeScript } = require('./parser');

module.exports = function apiDocsPlugin(context, options) {
  const { srcDir = 'src', include = ['**/*.ts'] } = options;

  return {
    name: 'docusaurus-plugin-api-docs',

    async loadContent() {
      const srcPath = path.join(context.siteDir, srcDir);

      // Parse TypeScript files
      const files = await fs.readdir(srcPath);
      const apiDocs = [];

      for (const file of files) {
        if (!file.endsWith('.ts')) continue;

        const filePath = path.join(srcPath, file);
        const content = await fs.readFile(filePath, 'utf-8');

        // Extract functions, classes, interfaces
        const parsed = parseTypeScript(content);
        apiDocs.push(...parsed);
      }

      return apiDocs;
    },

    async contentLoaded({ content, actions }) {
      const { createData, addRoute } = actions;

      // Create API reference pages
      await Promise.all(
        content.map(async (apiItem) => {
          const dataPath = await createData(`api-${apiItem.name}.json`, JSON.stringify(apiItem));

          addRoute({
            path: `/api/${apiItem.name}`,
            component: '@site/src/components/ApiDoc.js',
            exact: true,
            modules: { apiItem: dataPath },
          });
        })
      );

      // Create API index page
      const indexPath = await createData('api-index.json', JSON.stringify(content));

      addRoute({
        path: '/api',
        component: '@site/src/components/ApiIndex.js',
        exact: true,
        modules: { items: indexPath },
      });
    },
  };
};
```

## Using Plugin Data in Components

### usePluginData Hook

```javascript
import React from 'react';
import usePluginData from '@docusaurus/usePluginData';

export default function ChangelogWidget() {
  const { entries, latestVersion } = usePluginData('docusaurus-plugin-content-changelog');

  return (
    <div className="changelog-widget">
      <h3>Latest Release: v{latestVersion}</h3>
      <ul>
        {entries.slice(0, 3).map((entry) => (
          <li key={entry.id}>
            <a href={`/changelog/${entry.slug}`}>{entry.title}</a>
          </li>
        ))}
      </ul>
    </div>
  );
}
```

### useGlobalData Hook

```javascript
import React from 'react';
import useGlobalData from '@docusaurus/useGlobalData';

export default function AllPluginData() {
  const globalData = useGlobalData();

  // Access data from all plugins
  const changelogData = globalData['docusaurus-plugin-content-changelog'];
  const teamData = globalData['docusaurus-plugin-content-team'];

  return (
    <div>
      <p>Latest version: {changelogData.latestVersion}</p>
      <p>Team members: {teamData.members.length}</p>
    </div>
  );
}
```

## Package Structure

```json
{
  "name": "@org/docusaurus-plugin-content-changelog",
  "version": "1.0.0",
  "main": "index.js",
  "keywords": ["docusaurus", "plugin", "content", "changelog"],

  "dependencies": {
    "gray-matter": "^4.0.3",
    "fs-extra": "^11.0.0"
  },

  "peerDependencies": {
    "@docusaurus/core": "^3.0.0"
  }
}
```

## TypeScript Support

```typescript
// index.d.ts
import { Plugin, LoadContext } from '@docusaurus/types';

export interface ChangelogEntry {
  id: string;
  slug: string;
  title: string;
  version: string;
  date: string;
  type: 'feature' | 'fix' | 'breaking';
  body: string;
}

export interface PluginOptions {
  changelogPath?: string;
  routeBasePath?: string;
  include?: string[];
}

export interface PluginContent {
  entries: ChangelogEntry[];
  latestVersion: string;
}

declare const plugin: (context: LoadContext, options: PluginOptions) => Plugin<ChangelogEntry[]>;

export default plugin;
```

## Best Practices

1. **Validate frontmatter** - Ensure required fields exist
2. **Sort content logically** - By date, priority, or category
3. **Cache external data** - Avoid repeated API calls during dev
4. **Provide TypeScript types** - Better DX with autocomplete
5. **Handle errors gracefully** - Missing files, invalid data
6. **Create indexes** - List pages for browsing content
7. **Generate feeds** - RSS, JSON APIs for external consumption

## Common Patterns

### Pagination

```javascript
async contentLoaded({ content, actions }) {
  const { createData, addRoute } = actions;
  const pageSize = 10;
  const pageCount = Math.ceil(content.length / pageSize);

  for (let i = 0; i < pageCount; i++) {
    const pageContent = content.slice(i * pageSize, (i + 1) * pageSize);
    const dataPath = await createData(
      `page-${i + 1}.json`,
      JSON.stringify({
        items: pageContent,
        page: i + 1,
        totalPages: pageCount,
      })
    );

    addRoute({
      path: i === 0 ? '/items' : `/items/page/${i + 1}`,
      component: '@site/src/components/ItemList.js',
      exact: true,
      modules: { data: dataPath },
    });
  }
}
```

### Tagging/Categories

```javascript
// Group content by tags
const tagMap = new Map();

content.forEach((item) => {
  item.tags.forEach((tag) => {
    if (!tagMap.has(tag)) {
      tagMap.set(tag, []);
    }
    tagMap.get(tag).push(item);
  });
});

// Create tag pages
for (const [tag, items] of tagMap) {
  const dataPath = await createData(`tag-${tag}.json`, JSON.stringify(items));

  addRoute({
    path: `/tags/${tag}`,
    component: '@site/src/components/TagPage.js',
    modules: { items: dataPath },
  });
}
```

### Search Integration

```javascript
// Generate search index
async postBuild({ outDir, content }) {
  const searchIndex = content.map((item) => ({
    id: item.id,
    title: item.title,
    content: item.body,
    url: `/changelog/${item.slug}`,
  }));

  await fs.writeFile(
    path.join(outDir, 'search-index.json'),
    JSON.stringify(searchIndex)
  );
}
```
