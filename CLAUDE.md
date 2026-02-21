# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a personal blog website built with [Jekyll](https://jekyllrb.com/) using the [Chirpy](https://github.com/cotes2020/jekyll-theme-chirpy) theme. It is deployed to GitHub Pages at https://liaofushen.github.io.

## Common Commands

```bash
# Install dependencies
bundle install

# Build the site locally
bundle exec jekyll b

# Serve locally with live reload (development)
bundle exec jekyll s

# Serve with drafts visible
bundle exec jekyll s --drafts

# Clean build artifacts
bundle exec jekyll clean

# Test HTML (after building)
bundle exec htmlproofer _site --disable-external --ignore-urls "/^http:\/\/127.0.0.1/,/^http:\/\/0.0.0.0/,/^http:\/\/localhost/"
```

## Site Structure

- `_posts/` - Blog posts in Markdown format (YYYY-MM-DD-title.md)
- `_tabs/` - Static pages (about, categories, tags, archives)
- `_data/` - Configuration data (share, contact settings)
- `_config.yml` - Main Jekyll configuration
- `index.html` - Homepage entry point
- `.github/workflows/pages-deploy.yml` - CI/CD workflow for GitHub Pages

## Creating Posts

Create new posts in `_posts/` with the naming convention `YYYY-MM-DD-title.md`:

```yaml
---
title: Your Title
date: YYYY-MM-DD HH:MM:SS +0800
categories: [category1, category2]
tags: [tag1, tag2]
---

Your content here...
```

## Deployment

Push to the `main` branch to trigger automatic deployment via GitHub Actions. The workflow is defined in `.github/workflows/pages-deploy.yml`.
