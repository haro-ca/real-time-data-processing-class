# Website Brief: Carlos Alberto Haro - Personal Website

## Overview
Create a personal website for Carlos Alberto Haro (Data & Analytics Product Manager and Solutions Engineer) deployed on Cloudflare Pages at h1sort.com.

## Domain & Hosting
- **Domain**: h1sort.com (already configured on Cloudflare)
- **Zone ID**: 87e7430ccaa4c4e765c510a11aefd41a
- **Account ID**: e219844b0f8366d6adad4344d59c35e2
- **Hosting**: Cloudflare Pages (new project to be created)

## Site Structure
1. **CV Presentation** - Animated slides showcasing professional background
2. **Blog Section** - Technical posts, industry thoughts, project journey (minimal for now)
3. **Research Section** - Active and past research projects
4. **Contact Page** - Contact info and social links

## Design References
The site should draw inspiration from these existing designs:
- `/Users/Haro/Code/michel-slides/pitch.html` - Editorial acid theme with Fraunces serif + IBM Plex Mono
- `/Users/Haro/Code/cv/carlos-haro-cv.html` - Warm editorial theme with Fraunces + Work Sans

**Key Design Elements**:
- Editorial, sophisticated aesthetic
- Strong typography (serif display + clean body fonts)
- Subtle animations and micro-interactions
- Paper-like textures and overlays
- Muted color palettes with accent colors
- Viewport-fitting layouts (100vh slides)
- Smooth scroll-snap navigation for CV section

## CV Content (July 2025 - July 2026)

### Personal Info
- **Name**: Carlos Alberto Haro López
- **Title**: Data & Analytics Product Manager and Solutions Engineer
- **Email**: haro_ca@outlook.com
- **Phone**: +52 5549855334
- **Experience**: 8+ years

### Summary
8+ years of experience designing and implementing end-to-end data solutions across AWS, Azure, and GCP. Adept at bridging technical execution and business strategy to drive innovative and scalable data solutions, for sales and for execution.

### Highlights
- Currently working as a Product Manager and Solutions Engineer, overseeing the design and execution of data projects
- Diverse background as an individual contributor, spanning Data Engineering (2 years), Data Science (2 years), ML Engineering (1 year), and Software Engineering (1 year)—providing a comprehensive perspective across the data lifecycle
- Current role – Santander: AI Technical Product Manager

### Work Experience

**Santander AI** - Technical PM (2025 – present, extended to July 2026)
- **Project**: RAG Chatbot for branch network and contact center usage
- **Problem**: High cost-to-serve for clients caused by scattered knowledge around retail financial product characteristics and procedures
- **Product**: Cloud RAG system management and deployment. Includes Knowledge Base Owners management, prompt design, and evals strategy design and implementation
- **Role**: Product manager, Lead Engineer
- **Highlights**: Scaled evals from <100 to >1,000 while cutting eval time 10x through improved strategy
- **Outcome**: Version 1.0 scheduled for production deployment in August 2025

**Accenture Data & AI** - Manager (2024 – 2025)
- **Client**: International Bank Company
- **Project**: Cloud MLOps framework implementation
- **Problem**: Data science area capable of model development and deployment but facing slow iteration speed due to inexistent CI/CD (MLOps) pipelines
- **Product**: PaaS cloud MLOps configuration and deployment (e.g. Snowflake ML, VertexAI, Sagemaker, AzureML, Databricks). Additionally, migration, deployment, and monitoring of selected models to the new platform (from legacy vendor code to open source alternatives)
- **Role**: Product manager, Solution Architect, Lead Engineer
- **Highlights**: Legacy model development uses proprietary autoML modelling software, migrations are made custom, matching model results and architecture
- **Outcome**: V1.0 production deployment delivered. Further improvements being worked on

**Accenture Data & AI** - Consultant (2023)
- **Client**: International Insurance Company
- **Project**: End-to-end analytics platform assessment
- **Problem**: Current efforts on data platform modernization to cloud were giving low business results. Data warehouse/lake migrations presented high SLAs for incorporating new data (both for business and data science areas) due to bad relational modelling practices and inexistent modern analytics engineering framework
- **Product**: Prioritized roadmap for platform optimization based on technical assessment, covering the full data value chain, including OnPrem transactional DBs, OnPrem Data Warehouse, ongoing cloud migration, and all downstream BI consumption
- **Role**: Lead engineer
- **Highlights**: Highly complex architecture due to hybrid data pipelines interactions
- **Outcome**: Delivered. Currently in plans for implementation

**Accenture Data & AI** - Consultant (2022)
- **Client**: International Retailer
- **Project**: Omnichannel unified data model
- **Problem**: Omnichannel KPIs didn't have a centralized consumption tool. Both brick & mortar and electronic sales were consulted through different methodologies and software, joined through ad hoc procedures giving different results per area
- **Product**: Unified data model on the central cloud data warehouse, exposed via BI dashboard
- **Role**: Lead engineer and solution architect
- **Highlights**: Most data available on downstream storage (cloud data warehouse) didn't have enough granularity to reconcile a single data model. ETLs were designed and implemented from scratch in pyspark
- **Outcome**: Delivered. ETL pipelines and unified relational model still in use, dashboard faced UX/UI changes, underlying KPI semantic layer was kept intact

### Prior Roles
- **Software Engineer** (2020-2021): Development Bank
- **Data Scientist** (2018-2020): Central Tax Administration Office
- **Data Engineer** (2017): Boutique consulting agency

## Research Section

### GitHub Repositories to Showcase
1. **real-time-data-processing-class**: https://github.com/haro-ca/real-time-data-processing-class
2. **matmul**: https://github.com/haro-ca/matmul

### Research Content Structure
- **Active Projects**: Current research work, status updates, goals
- **Past Research**: Completed papers, findings, publications
- **Research Outputs**: Links to papers, datasets, code repositories

## Blog Section

### Content Types
- Technical posts (tutorials, how-to guides, code examples)
- Industry thoughts (insights, trends, commentary)
- Project journey (updates, behind-the-scenes work)

### Technical Requirements
- **Format**: Static Markdown
- **Build Process**: Convert Markdown to static HTML
- **Status**: Minimal section for now (structure ready, content to be added later)

## Technical Requirements

### Build & Deployment
- **Platform**: Cloudflare Pages
- **Static Site Generator**: Choose appropriate for Markdown support (e.g., Hugo, Jekyll, or custom build)
- **Deployment**: Connect to GitHub repository (haro-ca) once connected
- **Domain**: h1sort.com (already configured)

### Performance & UX
- Fast page loads
- Responsive design
- Smooth animations (respecting prefers-reduced-motion)
- Accessible navigation
- SEO-friendly structure

### Features to Consider
- RSS feed for blog
- Dark/light mode toggle
- Contact form or email integration
- Social media links
- Search functionality
- Filtering by topic/category

## Design Implementation Notes

### CV Animation Style
- Full presentation mode with slide-based navigation
- Smooth transitions between sections
- Animated entrance effects (fade, slide, scale)
- Progress indicator
- Keyboard navigation support
- Touch/swipe support for mobile

### Typography
- Display font: Serif (Fraunces or similar)
- Body font: Clean sans-serif (Work Sans, IBM Plex Mono, or similar)
- Monospace for code/technical elements

### Color Palette
- Inspired by reference designs:
  - Option A: Cream paper + charcoal ink + acid green accent
  - Option B: Warm cream + amber accent + charcoal text
- Muted, sophisticated tones with sharp accent colors

### Layout Principles
- Viewport-fitting (100vh for CV slides)
- No scrolling within slides
- Content density limits per slide
- Responsive breakpoints for different screen sizes

## Next Steps for Building Agent

1. **Set up Cloudflare Pages project**
2. **Choose and configure static site generator**
3. **Implement design system based on reference files**
4. **Create CV presentation with animations**
5. **Build blog structure with Markdown support**
6. **Create research section showcasing GitHub repos**
7. **Add contact page**
8. **Configure domain (h1sort.com)**
9. **Test deployment and functionality**
10. **Document content management workflow**

## Content Management

### CV Updates
- Extend timeline from July 2025 to July 2026
- Add any new projects, skills, or achievements from this period

### Blog Content
- Start with minimal structure
- Add posts progressively using Markdown
- Consider content categorization

### Research Updates
- Regular updates to active projects
- Add new repositories as they become available
- Link to publications and outputs

## Contact Information to Include
- Email: haro_ca@outlook.com
- Phone: +52 5549855334
- GitHub: https://github.com/haro-ca
- LinkedIn: (to be added)
- Other social links: (to be added)

---

**Generated for**: Website building agent
**Date**: July 4, 2026
**Domain**: h1sort.com
**Design References**: `/Users/Haro/Code/michel-slides/pitch.html`, `/Users/Haro/Code/cv/carlos-haro-cv.html`