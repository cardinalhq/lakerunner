# Rehype Plugins for Docusaurus

Rehype plugins transform **HTML content** after markdown has been converted to HTML. They operate on the HAST (HTML Abstract Syntax Tree).

## When to Use Rehype Plugins

- HTML post-processing and cleanup
- Adding wrapper divs or containers
- Accessibility improvements (ARIA attributes)
- Modifying HTML attributes
- Syntax highlighting customization
- Lazy loading images
- Adding classes to elements

## Remark vs Rehype

| Aspect       | Remark                     | Rehype                |
| ------------ | -------------------------- | --------------------- |
| **Input**    | Markdown                   | HTML                  |
| **AST**      | MDAST                      | HAST                  |
| **Timing**   | Before HTML conversion     | After HTML conversion |
| **Use Case** | Markdown syntax extensions | HTML manipulation     |
| **Example**  | Custom `[[term]]` syntax   | Add wrapper divs      |

## Complete Plugin Structure

```javascript
// index.js - Rehype plugin
const { visit } = require('unist-util-visit');
const { h } = require('hastscript');

module.exports = function rehypeCustomPlugin(options = {}) {
  const {
    wrapperClass = 'content-wrapper',
    addLazyLoading = true,
    externalLinkIcon = true,
  } = options;

  return function transformer(tree, file) {
    // Add lazy loading to images
    if (addLazyLoading) {
      visit(tree, 'element', (node) => {
        if (node.tagName === 'img') {
          node.properties.loading = 'lazy';
          node.properties.decoding = 'async';
        }
      });
    }

    // Add icon to external links
    if (externalLinkIcon) {
      visit(tree, 'element', (node) => {
        if (node.tagName === 'a' && node.properties.href) {
          const href = node.properties.href;

          if (href.startsWith('http') && !href.includes(options.siteUrl || '')) {
            // Add external link class
            node.properties.className = [...(node.properties.className || []), 'external-link'];

            // Add rel attributes for security
            node.properties.rel = 'noopener noreferrer';
            node.properties.target = '_blank';

            // Add icon element
            node.children.push({
              type: 'element',
              tagName: 'span',
              properties: { className: ['external-icon'] },
              children: [{ type: 'text', value: ' ↗' }],
            });
          }
        }
      });
    }

    // Wrap content sections
    visit(tree, 'element', (node, index, parent) => {
      if (node.tagName === 'div' && node.properties.className?.includes('markdown')) {
        // Wrap in custom container
        const wrapper = h(`div.${wrapperClass}`, {}, [node]);
        parent.children[index] = wrapper;
      }
    });

    return tree;
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
          rehypePlugins: [require('./plugins/my-rehype-plugin')],
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
          rehypePlugins: [
            [
              require('./plugins/my-rehype-plugin'),
              {
                wrapperClass: 'custom-wrapper',
                addLazyLoading: true,
                externalLinkIcon: true,
                siteUrl: 'https://mysite.com',
              },
            ],
          ],
        },
      },
    ],
  ],
};
```

## Common HTML Node Types

### Element Nodes

```javascript
{
  type: 'element',
  tagName: 'div',
  properties: {
    className: ['my-class'],
    id: 'my-id',
    dataCustom: 'value'
  },
  children: [...]
}
```

### Text Nodes

```javascript
{
  type: 'text',
  value: 'Text content'
}
```

### Common Elements

```javascript
// Link
{
  type: 'element',
  tagName: 'a',
  properties: {
    href: 'https://example.com',
    rel: 'noopener',
    target: '_blank'
  },
  children: [{ type: 'text', value: 'Link text' }]
}

// Image
{
  type: 'element',
  tagName: 'img',
  properties: {
    src: '/img/photo.jpg',
    alt: 'Description',
    loading: 'lazy'
  },
  children: []
}

// Heading
{
  type: 'element',
  tagName: 'h2',
  properties: { id: 'heading-id' },
  children: [{ type: 'text', value: 'Heading' }]
}
```

## Using hastscript

The `hastscript` library (`h()` function) makes creating HTML nodes easier:

```javascript
const { h } = require('hastscript');

// Create elements
const div = h('div', { className: 'container' }, [
  h('p', 'Paragraph text'),
  h('a', { href: '#' }, 'Link'),
]);

// Shorthand with classes and IDs
const header = h('div.header#main', [h('h1.title', 'Page Title')]);

// Result:
// <div class="header" id="main">
//   <h1 class="title">Page Title</h1>
// </div>
```

## Real-World Examples

### 1. Add Wrapper Divs to Code Blocks

```javascript
const { visit } = require('unist-util-visit');
const { h } = require('hastscript');

module.exports = function rehypeCodeWrapper() {
  return function transformer(tree) {
    visit(tree, 'element', (node, index, parent) => {
      if (node.tagName === 'pre') {
        // Get language from code element
        const codeNode = node.children[0];
        const language = codeNode?.properties?.className?.[0]?.replace('language-', '') || 'text';

        // Create wrapper with copy button
        const wrapper = h('div.code-block-wrapper', { dataLanguage: language }, [
          h('div.code-header', [
            h('span.language-label', language),
            h('button.copy-button', { type: 'button' }, 'Copy'),
          ]),
          node,
        ]);

        parent.children[index] = wrapper;
      }
    });

    return tree;
  };
};
```

### 2. Lazy Load Images with Blur Placeholder

```javascript
const { visit } = require('unist-util-visit');

module.exports = function rehypeLazyImages(options = {}) {
  const { blurDataURL = 'data:image/...' } = options;

  return function transformer(tree) {
    visit(tree, 'element', (node) => {
      if (node.tagName === 'img') {
        // Add lazy loading
        node.properties.loading = 'lazy';
        node.properties.decoding = 'async';

        // Add blur placeholder
        node.properties.style = `background-image: url('${blurDataURL}'); background-size: cover;`;

        // Wrap in picture element for responsive images
        const picture = {
          type: 'element',
          tagName: 'picture',
          properties: {},
          children: [
            // WebP source
            {
              type: 'element',
              tagName: 'source',
              properties: {
                srcset: node.properties.src.replace(/\.(jpg|png)$/, '.webp'),
                type: 'image/webp',
              },
              children: [],
            },
            // Original img
            node,
          ],
        };

        return picture;
      }
    });

    return tree;
  };
};
```

### 3. Add Accessibility Improvements

```javascript
const { visit } = require('unist-util-visit');

module.exports = function rehypeA11y() {
  return function transformer(tree) {
    visit(tree, 'element', (node) => {
      // Add ARIA labels to links without text
      if (node.tagName === 'a') {
        const hasText = node.children.some(
          (child) => child.type === 'text' || (child.type === 'element' && child.tagName !== 'img')
        );

        if (!hasText) {
          node.properties.ariaLabel = node.properties.href;
        }

        // Mark external links
        if (node.properties.href?.startsWith('http')) {
          node.properties.ariaLabel =
            `${node.properties.ariaLabel || ''} (opens in new tab)`.trim();
        }
      }

      // Ensure images have alt text
      if (node.tagName === 'img' && !node.properties.alt) {
        console.warn(`Image missing alt text: ${node.properties.src}`);
        node.properties.alt = 'Image'; // Fallback
      }

      // Add role to nav elements
      if (node.tagName === 'nav' && !node.properties.role) {
        node.properties.role = 'navigation';
      }
    });

    return tree;
  };
};
```

### 4. Add Reading Time Meta

```javascript
const { visit } = require('unist-util-visit');
const { h } = require('hastscript');

module.exports = function rehypeReadingTime() {
  return function transformer(tree, file) {
    let wordCount = 0;

    // Count words
    visit(tree, 'text', (node) => {
      wordCount += node.value.split(/\s+/).length;
    });

    // Calculate reading time (average 200 words per minute)
    const readingTime = Math.ceil(wordCount / 200);

    // Add to frontmatter or tree
    file.data.readingTime = readingTime;

    // Insert reading time element at the beginning
    if (tree.children[0]) {
      tree.children.unshift(
        h('div.reading-time', { dataMinutes: readingTime }, [h('span', `${readingTime} min read`)])
      );
    }

    return tree;
  };
};
```

### 5. Enhance Tables

```javascript
const { visit } = require('unist-util-visit');
const { h } = require('hastscript');

module.exports = function rehypeTables() {
  return function transformer(tree) {
    visit(tree, 'element', (node, index, parent) => {
      if (node.tagName === 'table') {
        // Wrap table in responsive container
        const wrapper = h('div.table-wrapper', [h('div.table-scroll', [node])]);

        // Add sortable classes to headers
        visit(node, 'element', (headerNode) => {
          if (headerNode.tagName === 'th') {
            headerNode.properties.className = [
              ...(headerNode.properties.className || []),
              'sortable',
            ];
            headerNode.properties.tabIndex = 0;
          }
        });

        parent.children[index] = wrapper;
      }
    });

    return tree;
  };
};
```

## Package Dependencies

```json
{
  "dependencies": {
    "unist-util-visit": "^4.0.0",
    "hastscript": "^7.0.0"
  },
  "peerDependencies": {
    "@docusaurus/core": "^3.0.0",
    "rehype": "^12.0.0"
  },
  "devDependencies": {
    "@types/hast": "^2.0.0",
    "jest": "^29.0.0"
  }
}
```

## TypeScript Definitions

```typescript
// index.d.ts
import { Plugin } from 'unified';
import { Root } from 'hast';

export interface RehypePluginOptions {
  wrapperClass?: string;
  addLazyLoading?: boolean;
  externalLinkIcon?: boolean;
  siteUrl?: string;
}

declare const rehypePlugin: Plugin<[RehypePluginOptions?], Root>;
export default rehypePlugin;
```

## Testing

```javascript
// __tests__/plugin.test.js
const rehype = require('rehype');
const customPlugin = require('../index');

describe('Rehype Custom Plugin', () => {
  const processor = rehype().use(customPlugin, {
    addLazyLoading: true,
  });

  it('adds lazy loading to images', async () => {
    const input = '<img src="/photo.jpg" alt="Photo" />';
    const result = await processor.process(input);

    expect(result.toString()).toContain('loading="lazy"');
  });

  it('adds external link icons', async () => {
    const input = '<a href="https://external.com">Link</a>';
    const result = await processor.process(input);

    expect(result.toString()).toContain('external-link');
    expect(result.toString()).toContain('target="_blank"');
  });
});
```

## Best Practices

1. **Use hastscript for creating nodes** - Cleaner than manual object creation
2. **Preserve existing properties** - Don't overwrite, merge instead
3. **Handle edge cases** - Check for missing children, properties
4. **Add security attributes** - Use `rel="noopener noreferrer"` for external links
5. **Maintain accessibility** - Add ARIA labels, alt text
6. **Wrap destructive changes** - Use options to enable/disable transformations
7. **Log warnings, not errors** - Don't crash on missing attributes

## Common Use Cases

### Responsive Images

Add `srcset`, lazy loading, and blur placeholders.

### Code Block Enhancement

Add copy buttons, language labels, line numbers.

### Link Processing

Add icons for external links, security attributes, analytics tracking.

### Accessibility

ARIA labels, semantic HTML, keyboard navigation.

### Performance

Lazy loading, async decoding, resource hints.

### SEO

Structured data, meta tags, Open Graph images.

## Debugging

```javascript
// Log all HTML elements
visit(tree, 'element', (node) => {
  console.log(node.tagName, node.properties);
});

// Log tree structure
console.log(JSON.stringify(tree, null, 2));
```

Use online AST explorers:

- https://astexplorer.net/ (select "HTML" and "rehype")
