/**
 * Shared footer component. Injects consistent footer into any page.
 *
 * Usage: <site-footer></site-footer>
 */

const FOOTER_STYLE_ID = 'footer-shared-css';
if (!document.getElementById(FOOTER_STYLE_ID)) {
  const style = document.createElement('style');
  style.id = FOOTER_STYLE_ID;
  style.textContent = `
    .site-footer {
      max-width: 940px; margin: 0 auto; padding: 2rem 2rem 2.5rem;
      border-top: 1px solid var(--border, #333);
      margin-top: 3rem;
    }
    .footer-grid {
      display: grid; grid-template-columns: repeat(3, 1fr);
      gap: 2rem; margin-bottom: 1.5rem;
    }
    .footer-col-title {
      font-size: 0.67rem; font-weight: 700; letter-spacing: 0.1em;
      text-transform: uppercase; color: var(--text-dim, #999);
      margin-bottom: 0.6rem;
    }
    .footer-col a {
      display: block; font-size: 0.8rem; color: var(--text-muted, #db7c26);
      text-decoration: none; line-height: 1.9; transition: color 0.15s;
    }
    .footer-col a:hover { color: var(--text, #f2ece4); }
    .footer-bottom {
      display: flex; justify-content: space-between; align-items: center;
      padding-top: 1rem; border-top: 1px solid var(--border, #333);
      font-size: 0.72rem; color: var(--text-dim, #999);
    }
    .footer-bottom a {
      color: var(--text-dim, #999); text-decoration: none;
      transition: color 0.15s;
    }
    .footer-bottom a:hover { color: var(--text, #f2ece4); }
    @media (max-width: 640px) {
      .footer-grid { grid-template-columns: 1fr 1fr; gap: 1.25rem; }
      .footer-bottom { flex-direction: column; gap: 0.5rem; text-align: center; }
    }
    @media (max-width: 400px) {
      .footer-grid { grid-template-columns: 1fr; }
    }
  `;
  document.head.appendChild(style);
}

class SiteFooter extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <footer class="site-footer">
        <div class="footer-grid">
          <div class="footer-col">
            <div class="footer-col-title">Download</div>
            <a href="https://github.com/afriedman412/petey-app/releases/latest" target="_blank" rel="noopener">Desktop App (Mac)</a>
            <a href="https://github.com/afriedman412/petey-app/releases/latest" target="_blank" rel="noopener">Desktop App (Windows)</a>
            <a href="https://hub.docker.com/r/afriedman412/petey" target="_blank" rel="noopener">Docker</a>
            <a href="https://pypi.org/project/petey/" target="_blank" rel="noopener">PyPI (CLI)</a>
          </div>
          <div class="footer-col">
            <div class="footer-col-title">Resources</div>
            <a href="/demos">Demos</a>
            <a href="/about">About</a>
            <a href="/about#keys">API Keys</a>
            <a href="/about#costs">Costs</a>
            <a href="/about#faq">FAQ</a>
          </div>
          <div class="footer-col">
            <div class="footer-col-title">Project</div>
            <a href="https://github.com/afriedman412/petey" target="_blank" rel="noopener">GitHub</a>
            <a href="/about#legal">Legal</a>
          </div>
        </div>
        <div class="footer-bottom">
          <span>Petey &copy; ${new Date().getFullYear()}</span>
          <a href="/about#legal">Disclaimers</a>
        </div>
      </footer>
    `;
  }
}

customElements.define('site-footer', SiteFooter);
