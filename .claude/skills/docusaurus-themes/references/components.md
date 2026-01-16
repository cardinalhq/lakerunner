# Common Docusaurus Components to Swizzle

## Layout Components

### Footer

**Path:** `src/theme/Footer/`
**Use case:** Customize footer content, links, copyright, social media icons

**Example customization:**

```tsx
import React from 'react';
import Footer from '@theme-original/Footer';

export default function FooterWrapper(props) {
  return (
    <>
      <Footer {...props} />
      <div className="custom-footer-section">{/* Add custom footer content */}</div>
    </>
  );
}
```

### Navbar

**Path:** `src/theme/Navbar/`
**Use case:** Modify navigation bar appearance, add custom elements, change behavior

**Common modifications:**

- Add custom logo behavior
- Insert additional navigation items
- Modify mobile menu behavior

### Layout

**Path:** `src/theme/Layout/`
**Use case:** Wrap all pages with custom providers, analytics, or global components

## Documentation Components

### DocItem

**Path:** `src/theme/DocItem/`
**Use case:** Customize documentation page layout, add custom elements to doc pages

**Common modifications:**

- Add custom headers or footers to docs
- Insert advertisements or announcements
- Add custom metadata display

### DocSidebar

**Path:** `src/theme/DocSidebar/`
**Use case:** Adjust sidebar behavior, styling, and interaction

**Common modifications:**

- Custom sidebar item rendering
- Add search functionality to sidebar
- Change collapsible behavior

### TOC (Table of Contents)

**Path:** `src/theme/TOC/`
**Use case:** Modify table of contents display, add custom elements

**Common modifications:**

- Add "Edit this page" links
- Customize heading filtering
- Add custom TOC actions

## Blog Components

### BlogPostPage

**Path:** `src/theme/BlogPostPage/`
**Use case:** Customize blog post layout and structure

### BlogListPage

**Path:** `src/theme/BlogListPage/`
**Use case:** Modify blog list view, pagination, filtering

## Code Components

### CodeBlock

**Path:** `src/theme/CodeBlock/`
**Use case:** Customize code block rendering, add features like copy button customization

### MDXComponents

**Path:** `src/theme/MDXComponents/`
**Use case:** Override or extend default MDX components used in markdown

## Best Practices by Component

### Safe to Wrap (Low Risk)

- Footer - Usually safe to wrap for adding content
- Navbar - Wrapping is often sufficient for minor changes
- Layout - Good candidate for wrapping to add providers

### Consider Ejecting (When Needed)

- CodeBlock - Often need full control for custom highlighting
- DocItem - May need eject for significant layout changes
- BlogPostPage - Eject for custom post templates

## Swizzle Safety Levels

Docusaurus marks components with safety levels:

- **Safe**: Safe to swizzle, low breaking change risk
- **Unsafe**: May break with Docusaurus updates
- **Forbidden**: Cannot be swizzled (internal components)

Check safety level:

```bash
npm run swizzle @docusaurus/theme-classic -- --list
```

Components are marked with their safety level in the list output.

## Component Hierarchies

Understanding component relationships helps you choose what to swizzle:

```
Layout (top-level wrapper)
├── Navbar
├── DocPage
│   ├── DocSidebar
│   └── DocItem
│       └── TOC
└── Footer
```

**Tip:** Swizzle at the lowest level that meets your needs to minimize maintenance burden.

## Examples by Use Case

### Use Case: Add Custom Analytics

**Component:** Layout
**Mode:** Wrap
**Why:** Wrapping Layout lets you add providers without replacing core functionality

### Use Case: Custom Footer Design

**Component:** Footer
**Mode:** Eject
**Why:** Significant design changes usually require full control

### Use Case: Add Version Badge to Docs

**Component:** DocItem
**Mode:** Wrap
**Why:** Can inject custom elements without modifying core doc rendering

### Use Case: Custom Syntax Highlighting

**Component:** CodeBlock
**Mode:** Eject
**Why:** Deep integration with highlighting engine requires full access
