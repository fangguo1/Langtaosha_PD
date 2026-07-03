# Langtaosha Search UI Design System

## Design Philosophy

Langtaosha is an academic preprint platform.

The search page should feel:

* Scholarly
* Calm
* Lightweight
* Trustworthy
* Similar to arXiv / bioRxiv rather than Google Scholar

---

# Color Palette

## Primary Brand Color

Used for:

* Logo
* Primary buttons
* Active tabs
* Hyperlinks

```css
--primary: #0F4C81;
```

Deep academic blue.

---

## Secondary Color

Used for:

* Hover state
* Badges
* DOI tags

```css
--secondary: #2C7FB8;
```

---

## Accent Color

Used sparingly.

Examples:

* Citation count
* Selected filters
* Recommendation labels

```css
--accent: #D98C2B;
```

Warm golden orange.

---

## Background Colors

Main page background

```css
--bg-main: #FAFBFC;
```

Card background

```css
--bg-card: #FFFFFF;
```

Sidebar background

```css
--bg-sidebar: #F5F7F9;
```

---

# Text Colors

Primary text

```css
--text-primary: #1A1A1A;
```

Secondary text

```css
--text-secondary: #5B6675;
```

Metadata text

```css
--text-meta: #8A94A6;
```

---

# Border Colors

```css
--border-light: #E6EAF0;

--border-medium: #D7DDE5;
```

---

# Search Bar

Background

```css
#FFFFFF
```

Border

```css
#D7DDE5
```

Focused

```css
border-color:#0F4C81;

box-shadow:
0 0 0 3px rgba(15,76,129,0.12);
```

Height

```css
52px
```

Radius

```css
12px
```

---

# Buttons

Primary

Background

```css
#0F4C81
```

Text

```css
white
```

Hover

```css
#0A3B66
```

Secondary

```css
background:white;

border:1px solid #D7DDE5;

color:#0F4C81;
```

---

# Cards

Paper card

```css
background:white;

border:1px solid #E6EAF0;

border-radius:12px;

padding:24px;
```

Hover

```css
box-shadow:
0 4px 16px rgba(0,0,0,0.05);
```

---

# Typography

Font stack

```css
Inter,

Helvetica Neue,

Arial,

sans-serif
```

Title

```css
font-size:22px;

font-weight:600;
```

Authors

```css
font-size:15px;

font-weight:400;
```

Abstract

```css
font-size:15px;

line-height:1.7;
```

Metadata

```css
font-size:13px;

color:#8A94A6;
```

---

# Search Result Layout

Desktop

```text
┌──────────────────────────────┐
│ Sidebar Filters              │
│                              │
│ Subject                      │
│ Date                         │
│ Author                       │
│ License                      │
│                              │
└──────────┬───────────────────┘
           │


┌─────────────────────────────────────────────┐
│ Search Bar                                  │
├─────────────────────────────────────────────┤
│                                             │
│ Paper Card                                  │
│                                             │
├─────────────────────────────────────────────┤
│                                             │
│ Paper Card                                  │
│                                             │
└─────────────────────────────────────────────┘
```

Sidebar width

```css
280px
```

Content max width

```css
1100px
```

---

# Components

DOI badge

```css
background:#EDF4FA;

color:#0F4C81;

border-radius:999px;
```

Preprint badge

```css
background:#FFF4E8;

color:#D98C2B;
```

License badge

```css
background:#F3F5F7;

color:#5B6675;
```

---

# Motion

Transition

```css
transition:all 0.2s ease;
```

No animations longer than

```css
300ms
```

---

# Overall Impression

Keywords

```text
Academic

Minimal

Nature-like

bioRxiv-inspired

Trustworthy

Humanities + Science

Soft Blue
```
