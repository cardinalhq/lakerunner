# Remark Plugins for Docusaurus

Remark plugins transform **markdown content** during the parsing phase, before it's converted to HTML. They operate on the MDAST (Markdown Abstract Syntax Tree).

## When to Use Remark Plugins

- Custom markdown syntax (e.g., `%%term%%` for glossary terms)
- Link processing and validation
- Auto-generating content from markdown patterns
- Adding metadata or classes to markdown elements
- Content transformation before HTML rendering

## Complete Plugin Structure

```javascript
// index.js - Main plugin file
const { visit } = require('unist-util-visit');
const fs = require('fs');
const path = require('path');

module.exports = function remarkCustomPlugin(options = {}) {
  // Validate and process options
  const {
    pattern = /%%(.+?)%%/g,
    dataFile = './data/terms.json',
    componentName = 'CustomTooltip',
  } = options;

  // Load external data if needed
  let glossaryData = {};
  if (fs.existsSync(dataFile)) {
    glossaryData = JSON.parse(fs.readFileSync(dataFile, 'utf-8'));
  }

  // Return the transformer function
  return async function transformer(ast, vfile) {
    const filePath = vfile.path || '';

    // Visit specific node types
    visit(ast, 'text', (node, index, parent) => {
      const matches = [...node.value.matchAll(pattern)];

      if (matches.length === 0) return;

      // Build replacement nodes
      const newNodes = [];
      let lastIndex = 0;

      matches.forEach((match) => {
        const [fullMatch, termKey] = match;
        const startIndex = match.index;

        // Add text before match
        if (startIndex > lastIndex) {
          newNodes.push({
            type: 'text',
            value: node.value.slice(lastIndex, startIndex),
          });
        }

        // Add custom component node
        const termData = glossaryData[termKey];
        if (termData) {
          newNodes.push({
            type: 'jsx',
            value: `<${componentName} term="${termKey}" tooltip="${termData.tooltip}">${termData.display}</${componentName}>`,
          });
        } else {
          // Fallback if term not found
          newNodes.push({
            type: 'text',
            value: fullMatch,
          });
        }

        lastIndex = startIndex + fullMatch.length;
      });

      // Add remaining text
      if (lastIndex < node.value.length) {
        newNodes.push({
          type: 'text',
          value: node.value.slice(lastIndex),
        });
      }

      // Replace the original node
      parent.children.splice(index, 1, ...newNodes);
    });

    // Visit links
    visit(ast, 'link', (node) => {
      if (node.url.endsWith('.md')) {
        // Transform internal markdown links
        node.data = node.data || {};
        node.data.hProperties = {
          className: 'internal-link',
          'data-internal': true,
        };
      }
    });

    return ast;
  };
};
```

## Configuration in docusaurus.config.js

```javascript
// Basic configuration
module.exports = {
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          remarkPlugins: [require('./plugins/my-remark-plugin')],
        },
      },
    ],
  ],
};

// With options
module.exports = {
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          remarkPlugins: [
            [
              require('./plugins/my-remark-plugin'),
              {
                pattern: /\[\[(.+?)\]\]/g,
                dataFile: './glossary.json',
                componentName: 'GlossaryTerm',
              },
            ],
          ],
        },
      },
    ],
  ],
};

// Execute BEFORE default Docusaurus plugins
module.exports = {
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          beforeDefaultRemarkPlugins: [require('./plugins/my-remark-plugin')],
        },
      },
    ],
  ],
};
```

## Common Node Types

### Text Nodes

```javascript
{
  type: 'text',
  value: 'Some text content'
}
```

### Link Nodes

```javascript
{
  type: 'link',
  url: 'https://example.com',
  children: [{ type: 'text', value: 'Link text' }]
}
```

### Paragraph Nodes

```javascript
{
  type: 'paragraph',
  children: [...]
}
```

### JSX Nodes (for MDX)

```javascript
{
  type: 'jsx',
  value: '<CustomComponent prop="value">Content</CustomComponent>'
}
```

### Heading Nodes

```javascript
{
  type: 'heading',
  depth: 2,  // h2
  children: [{ type: 'text', value: 'Heading text' }]
}
```

## Using unist-util-visit

```javascript
const { visit } = require('unist-util-visit');

// Visit all nodes of a specific type
visit(ast, 'link', (node) => {
  console.log(node.url);
});

// Visit multiple types
visit(ast, ['link', 'image'], (node) => {
  console.log(node.type, node.url);
});

// Visit with index and parent access
visit(ast, 'text', (node, index, parent) => {
  // Modify parent.children to replace nodes
  parent.children[index] = newNode;
});

// Visit all nodes
visit(ast, (node) => {
  if (node.type === 'link' && node.url.startsWith('http')) {
    // Process external links
  }
});
```

## Real-World Example: Glossary Plugin

Based on docusaurus-plugin-glossary pattern:

```javascript
// plugins/glossary-plugin.js
const { visit } = require('unist-util-visit');
const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

module.exports = function glossaryPlugin(options = {}) {
  const {
    termsDir = './docs/terms',
    docsDir = './docs',
    glossaryFilepath = './docs/glossary.md',
  } = options;

  // Load all term files
  const loadTerms = () => {
    const terms = {};
    const termFiles = fs.readdirSync(termsDir);

    termFiles.forEach((file) => {
      if (!file.endsWith('.md')) return;

      const content = fs.readFileSync(path.join(termsDir, file), 'utf-8');
      const [, frontmatter, body] = content.match(/^---\n([\s\S]+?)\n---\n([\s\S]*)$/);

      const meta = yaml.load(frontmatter);
      terms[meta.id] = {
        title: meta.title,
        hoverText: meta.hoverText || body.slice(0, 200),
        path: `terms/${file.replace('.md', '')}`,
      };
    });

    return terms;
  };

  return function transformer(ast, vfile) {
    const terms = loadTerms();

    // Convert [[term]] syntax to tooltipped links
    visit(ast, 'text', (node, index, parent) => {
      const matches = [...node.value.matchAll(/\[\[(.+?)\]\]/g)];

      if (matches.length === 0) return;

      const newNodes = [];
      let lastIndex = 0;

      matches.forEach((match) => {
        const [fullMatch, termKey] = match;
        const term = terms[termKey];

        if (!term) {
          console.warn(`Term not found: ${termKey}`);
          return;
        }

        // Add text before match
        if (match.index > lastIndex) {
          newNodes.push({
            type: 'text',
            value: node.value.slice(lastIndex, match.index),
          });
        }

        // Add glossary link with tooltip
        newNodes.push({
          type: 'jsx',
          value: `<GlossaryTerm term="${termKey}" tooltip="${term.hoverText}" href="/${term.path}">${term.title}</GlossaryTerm>`,
        });

        lastIndex = match.index + fullMatch.length;
      });

      // Remaining text
      if (lastIndex < node.value.length) {
        newNodes.push({
          type: 'text',
          value: node.value.slice(lastIndex),
        });
      }

      parent.children.splice(index, 1, ...newNodes);
    });

    return ast;
  };
};
```

## Package Dependencies

```json
{
  "dependencies": {
    "unist-util-visit": "^4.0.0"
  },
  "peerDependencies": {
    "@docusaurus/core": "^3.0.0",
    "remark": "^13.0.0"
  },
  "devDependencies": {
    "@types/unist": "^2.0.0",
    "jest": "^29.0.0"
  }
}
```

## TypeScript Definitions

```typescript
// index.d.ts
import { Plugin } from 'unified';
import { Root } from 'mdast';

export interface RemarkPluginOptions {
  pattern?: RegExp;
  dataFile?: string;
  componentName?: string;
  termsDir?: string;
  docsDir?: string;
  glossaryFilepath?: string;
}

declare const remarkPlugin: Plugin<[RemarkPluginOptions?], Root>;
export default remarkPlugin;
```

## Testing

```javascript
// __tests__/plugin.test.js
const remark = require('remark');
const remarkMdx = require('remark-mdx');
const glossaryPlugin = require('../index');

describe('Glossary Plugin', () => {
  const processor = remark().use(remarkMdx).use(glossaryPlugin, {
    termsDir: './__fixtures__/terms',
  });

  it('transforms glossary syntax', async () => {
    const input = 'This is a [[test-term]] example.';
    const result = await processor.process(input);

    expect(result.toString()).toContain('<GlossaryTerm');
    expect(result.toString()).toContain('test-term');
  });

  it('handles missing terms gracefully', async () => {
    const input = 'This is a [[missing-term]] example.';
    const result = await processor.process(input);

    // Should leave unmatched terms as-is or show warning
    expect(result.toString()).toBeTruthy();
  });
});
```

## Best Practices

1. **Keep dependencies minimal** - Only use `unist-util-visit` for traversal
2. **Cache external data** - Load files once, not per-transform
3. **Handle missing data gracefully** - Don't crash on missing terms/files
4. **Preserve node position** - Maintain `position` property for error reporting
5. **Use JSX nodes for components** - Type `jsx` integrates with MDX
6. **Validate options** - Provide sensible defaults
7. **Support async operations** - Use `async/await` when reading files
8. **Add TypeScript definitions** - Improves developer experience

## Common Patterns

### Auto-linking Terms

Transform text patterns into links automatically.

### Custom Syntax

Add markdown extensions like `::note[text]` or `%%term%%`.

### Content Generation

Generate tables, lists, or summaries from markdown structure.

### Link Validation

Check internal links exist, add attributes to external links.

### Metadata Injection

Add frontmatter data as HTML attributes or classes.

## Debugging

```javascript
// Add logging to see AST structure
visit(ast, (node) => {
  console.log(JSON.stringify(node, null, 2));
});

// Log only specific types
visit(ast, 'link', (node) => {
  console.log('Link:', node.url);
});
```

Use online AST explorers:

- https://astexplorer.net/ (select "Markdown" and "remark")
