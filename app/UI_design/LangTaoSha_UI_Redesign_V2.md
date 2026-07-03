# LangTaoSha Smart Search UI Redesign V2

> Goal: Redesign the entire Smart Search page so it feels like a native
> page of the LangTaoSha Preprint Server rather than a standalone
> application.

------------------------------------------------------------------------

# Overall Design Philosophy

The search page should inherit the official LangTaoSha design language.

Keywords:

-   Professional
-   Academic
-   Trustworthy
-   Modern
-   Minimal
-   Calm
-   Information-first

Avoid startup-style gradients, glassmorphism, neon colors, oversized
rounded cards, and playful UI.

------------------------------------------------------------------------

# Brand Identity

## Primary Colors

  Usage              Color
  ------------------ ---------
  Brand Blue         #244C94
  Brand Blue Hover   #1F4385
  Brand Gold         #F2D16B
  White              #FFFFFF
  Background         #F7F9FC
  Border             #E4E8F0
  Text               #1F2937
  Secondary Text     #6B7280

------------------------------------------------------------------------

# Typography

Use Inter (fallback: system-ui).

Hierarchy:

  Element         Size       Weight
  --------------- ---------- --------
  Hero Title      56--64px   800
  Section Title   30px       700
  Card Title      22px       700
  Body            16px       400
  Caption         14px       500

Maintain generous whitespace.

------------------------------------------------------------------------

# Page Layout

    ---------------------------------------------------
    Official Navigation Bar (existing)
    ---------------------------------------------------

    Hero Section
    ---------------------------------------------------

    Search Box
    ---------------------------------------------------

    Filter Bar

    ---------------------------------------------------

    Search Results

    ---------------------------------------------------

    Footer

Maximum content width:

    1280px

Centered.

------------------------------------------------------------------------

# Hero Section

Use a two-column layout.

    Logo

    LangTaoSha Academic Search

    Langtaosha Smart Search

    We try to make academic research understand you more.

Reuse

``` html
<div class="header-left">
    <div class="logo-link">
        <img
            src="/lib/ui-library/src/resources/ltslogo_new.png"
            class="logo-image"
        />
    </div>
</div>
```

Hero background:

``` css
background:#244C94;
```

Hero padding:

    72px 0

Hero kicker:

-   LangTaoSha → Gold
-   Academic Search → White

------------------------------------------------------------------------

# Search Box

The search box should become the visual centerpiece.

Requirements:

-   Width 900--1000px
-   Height 60px
-   White background
-   1px border
-   Large placeholder
-   Rounded radius 14px
-   Soft shadow

Search button:

-   Brand blue
-   White text
-   Hover darker blue

Suggested layout

    +--------------------------------------------------------+
    | 🔍 Search preprints, concepts, authors...    [Search] |
    +--------------------------------------------------------+

------------------------------------------------------------------------

# Filter Bar

Place directly below the search box.

Horizontal chips.

Examples:

    Dense

    Sparse

    Hybrid

    Expanded Sparse

    Author

    Concept

    Recent

Selected state:

-   Brand blue background
-   White text

Unselected:

-   White background
-   Border

------------------------------------------------------------------------

# Search Results

Use academic paper cards.

Card style:

    --------------------------------------------------

    Paper Title

    Authors

    Journal / Source

    Abstract

    Matched Concepts

    Tags

    Actions

    --------------------------------------------------

Card:

``` css
border-radius:16px;
background:white;
border:1px solid #E4E8F0;
```

Spacing:

    24px

between cards.

Hover:

-   Slight lift
-   Border blue

------------------------------------------------------------------------

# Result Header

Above the cards:

    235 Results

    Query Time: 183 ms

    Dense Retrieval

Small grey metadata.

------------------------------------------------------------------------

# Keyword Highlight

Highlight matched query terms.

Background:

    #FFF3BF

Text:

    inherit

Bold.

------------------------------------------------------------------------

# Concept Matches

If ontology matching exists, display:

    Matched Concepts

    Kidney
    Cell Adhesion
    Protein

as pills.

Blue outline.

------------------------------------------------------------------------

# Evidence Panel

If expanded sparse retrieval is used, show:

    Matched Through

    renal → kidney

    adhesion protein

    cell adhesion molecule

inside a subtle bordered box.

------------------------------------------------------------------------

# Sidebar (Optional)

Desktop only.

Contains:

-   Search Mode
-   Filters
-   Year
-   Source
-   Sort

Sticky while scrolling.

------------------------------------------------------------------------

# Pagination

Centered.

Use official style.

Previous

1 2 3 4 5

Next

------------------------------------------------------------------------

# Empty State

Illustration optional.

Message:

    No results found.

    Try another keyword or search concept.

Button:

    Clear Filters

------------------------------------------------------------------------

# Loading State

Skeleton cards.

Do not use spinners.

------------------------------------------------------------------------

# Motion

Use subtle animation only.

Hover:

150ms

Page transitions:

200ms

No excessive motion.

------------------------------------------------------------------------

# Responsive

Desktop: 1280px

Tablet: Search bar shrinks

Mobile:

Everything stacks vertically.

------------------------------------------------------------------------

# Components to Reuse

Reuse existing components whenever possible:

-   Logo
-   Navigation bar
-   Brand colors
-   Buttons
-   Typography
-   Footer

Avoid introducing a separate visual language.

------------------------------------------------------------------------

# Deliverables

Refactor the frontend to create a cohesive design system that feels like
a natural extension of the official LangTaoSha website.

Preserve all existing search functionality.

Only improve:

-   Layout
-   Visual hierarchy
-   Typography
-   Colors
-   Component styling
-   Responsiveness
-   UX consistency

Do not change backend APIs or business logic.
