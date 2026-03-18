# API Modules (Consolidated)

This directory is the consolidated API-only entry layer for the repository.

## Preserved API Domains
- `api_modules.ads_report`: unified ads-report facade.
- `api_modules.tiktok`: TikTok app/auth loaders.
- `api_modules.ads`: low-level ads channels HTTP client factory.
- `api_modules.common`: shared HTTP request helper.

## Design Goal
- Keep API request capabilities.
- Reduce legacy surface and duplicated wrappers.
- Provide a small and stable import path for future services/jobs.
