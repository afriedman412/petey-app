/**
 * Shared navbar component. Injects consistent nav into any page.
 *
 * Usage: <nav-bar active="extractor"></nav-bar>
 *
 * Valid active values: extractor, runs, settings, about
 */

const GITHUB_ICON = '<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>';

const NAV_LINKS = [
  { id: 'extractor', label: 'Extractor', href: '/' },
  { id: 'guide', label: 'Guide', href: '/guide' },
  { id: 'runs', label: 'Runs', href: '/runs/page' },
  { id: 'settings', label: 'Settings', href: '/settings/page' },
  { id: 'about', label: 'About', href: '/about' },
];

// Inject shared nav CSS once
const NAV_STYLE_ID = 'navbar-shared-css';
if (!document.getElementById(NAV_STYLE_ID)) {
  const style = document.createElement('style');
  style.id = NAV_STYLE_ID;
  style.textContent = `
    .nav-hero { margin-bottom: 2.5rem; }
    .nav-hero-top {
      display: flex; justify-content: space-between; align-items: flex-end;
      margin-bottom: 0.45rem;
    }
    .nav-hero h1 {
      font-family: 'DM Serif Display', Georgia, serif;
      font-size: 2.4rem; font-weight: 400; letter-spacing: -0.01em; line-height: 1;
    }
    .nav-hero h1 .dot { color: var(--accent, #d8572a); }
    .nav-hero .hero-sub {
      font-size: 0.82rem; color: var(--text-dim, #999); letter-spacing: 0.02em;
    }
    .page-top-link {
      font-size: 0.78rem; color: var(--text-muted, #db7c26); text-decoration: none;
      letter-spacing: 0.03em; transition: color 0.15s; align-self: flex-end;
      padding-bottom: 0.2rem;
    }
    .page-top-link:hover { color: var(--text, #f2ece4); }
    .page-top-link.active { color: var(--text, #f2ece4); font-weight: 600; }
  `;
  document.head.appendChild(style);
}

class NavBar extends HTMLElement {
  connectedCallback() {
    const active = this.getAttribute('active') || '';
    const subtitle = this.getAttribute('subtitle') || '';

    let linksHtml = NAV_LINKS.map(link => {
      const cls = link.id === active ? 'page-top-link active' : 'page-top-link';
      return `<a href="${link.href}" class="${cls}">${link.label}</a>`;
    }).join('\n        ');

    linksHtml += `\n        <a href="https://github.com/afriedman412/petey" target="_blank" rel="noopener" class="page-top-link" title="GitHub" style="padding-bottom:0;">${GITHUB_ICON}</a>`;
    linksHtml += `\n        <a href="#" class="page-top-link" id="signOutLink" style="display:none;">Sign out</a>`;

    this.innerHTML = `
      <div class="nav-hero">
        <div class="nav-hero-top">
          <h1><a href="/" style="color:inherit; text-decoration:none;" ${active === 'extractor' ? 'id="logoReset"' : ''}>Petey<span class="dot">.</span></a></h1>
          <nav style="display:flex; gap:1.25rem; align-items:flex-end; padding-bottom:0.2rem;">
            ${linksHtml}
          </nav>
        </div>
        ${subtitle ? `<p class="hero-sub">${subtitle}</p>` : ''}
      </div>
    `;
  }
}

customElements.define('nav-bar', NavBar);
