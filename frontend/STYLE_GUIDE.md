# Frontend Style Guide

## Goals

- Prefer React + Tailwind utility classes for layout and styling.
- Keep styling colocated with components to reduce context switching.
- Minimize page-level CSS files; use global CSS only for tokens and shared primitives.

## Rules

- Use Tailwind utilities directly in `className` for one-off layout/style.
- Reuse shared primitive classes from `src/index.css` (`btn-brand`, `btn-subtle`, `input-base`, `section-block`, etc.).
- Avoid creating new page-specific `.css` files unless a style cannot be expressed by Tailwind utilities.
- Use component extraction when a class set is repeated 3+ times.
- Keep spacing/radius/typography consistent with existing design tokens in `:root`.

## Layout Conventions

- Page shell: `min-h-screen bg-[#f4f6f8] text-slate-900`.
- Primary card block: `section-block`.
- Form grid: `grid gap-3 md:grid-cols-2` and `md:col-span-2` for full-width fields.
- Content split: `xl:grid-cols-[minmax(420px,48%)_minmax(380px,52%)]`.

## Component Conventions

- Primary action: `btn-brand`.
- Secondary action: `btn-subtle`.
- Borderless highlighted action: `btn-ghost-brand`.
- Inputs and selects: `input-base`.
- Labels and help text: `field-label`, `panel-subtitle`.

## JSON/Code Display

- Use Tailwind typography/color utilities directly in JSX for syntax coloring and code blocks.
- Avoid standalone CSS files for simple color/token mapping.

## Migration Checklist (for old pages)

- Remove unused `*.css` imports.
- Replace page-local class definitions with Tailwind utility classes.
- Keep only shared/global primitives in `src/index.css`.
- Run `npm run build` after migration to verify class extraction/build output.
