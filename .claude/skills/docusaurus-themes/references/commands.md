# Docusaurus Swizzle Commands Reference

## Basic Commands

### Interactive Swizzling

Browse and select components interactively:

```bash
npm run swizzle
```

This will prompt you to:

1. Select a theme (e.g., @docusaurus/theme-classic)
2. Choose a component to swizzle
3. Select wrap or eject mode

### List Available Components

See all swizzlable components from a theme:

```bash
npm run swizzle @docusaurus/theme-classic -- --list
```

### Safe Swizzle (Wrap)

Wrap a component to add customizations while keeping the original:

```bash
npm run swizzle @docusaurus/theme-classic ComponentName -- --wrap
```

**When to use wrap:**

- You want to add functionality without replacing the original
- You need to preserve upgrade compatibility
- You're making minor customizations

### Unsafe Swizzle (Eject)

Eject to fully replace and customize a component:

```bash
npm run swizzle @docusaurus/theme-classic ComponentName -- --eject
```

**When to use eject:**

- You need full control over component internals
- You're making significant structural changes
- You understand you'll need to manually merge updates

## Command Options

- `--wrap` - Safe swizzle mode (wraps original component)
- `--eject` - Unsafe swizzle mode (copies full source)
- `--list` - List all available components to swizzle
- `--typescript` - Generate TypeScript files (default)
- `--javascript` - Generate JavaScript files

## File Locations

After swizzling, components are created in:

- `src/theme/ComponentName/` - Directory-based components (with index.tsx)
- `src/theme/ComponentName.tsx` - Single-file components

## Examples

### Example: Swizzle Footer

```bash
npm run swizzle @docusaurus/theme-classic Footer -- --eject
```

After running this, edit the footer in `src/theme/Footer/index.tsx`.

### Example: Wrap Navbar

```bash
npm run swizzle @docusaurus/theme-classic Navbar -- --wrap
```

This creates a wrapper in `src/theme/Navbar/index.tsx` that imports and extends the original.

### Example: List and Choose

```bash
# First, see what's available
npm run swizzle @docusaurus/theme-classic -- --list

# Then swizzle the component you need
npm run swizzle @docusaurus/theme-classic DocItem -- --wrap
```

## Troubleshooting

**Component not found:**

- Ensure you're using the correct theme package name
- Run `--list` to see available components

**TypeScript errors after swizzling:**

- Check that you have the correct type definitions installed
- Ensure your tsconfig.json includes the src/theme directory

**Changes not appearing:**

- Clear the `.docusaurus` cache: `npm run clear`
- Restart the development server
