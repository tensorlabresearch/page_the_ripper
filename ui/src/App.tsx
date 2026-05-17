import { NavLink, Route, Routes, Navigate } from 'react-router-dom';
import ScannersPage from './pages/ScannersPage';
import NewScanPage from './pages/NewScanPage';
import JobsPage from './pages/JobsPage';
import SystemPage from './pages/SystemPage';
import CropPage from './pages/CropPage';
import CompositePage from './pages/CompositePage';

function NavItem({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `rounded-md px-3 py-1.5 text-sm font-medium transition-colors ${
          isActive
            ? 'bg-brand-500/15 text-brand-400'
            : 'text-slate-300 hover:bg-slate-800 hover:text-slate-100'
        }`
      }
    >
      {children}
    </NavLink>
  );
}

export default function App() {
  return (
    <div className="flex min-h-full flex-col">
      <header className="border-b border-slate-800 bg-slate-950/80 backdrop-blur">
        <div className="mx-auto flex max-w-6xl items-center gap-6 px-4 py-3">
          <div className="flex items-center gap-2">
            <div className="grid h-8 w-8 place-items-center rounded-md bg-brand-500/15 text-brand-400">
              <svg viewBox="0 0 24 24" fill="none" className="h-5 w-5" stroke="currentColor" strokeWidth="2">
                <path d="M7 3h7l5 5v13H7z" strokeLinejoin="round" />
                <path d="M14 3v5h5" strokeLinejoin="round" />
                <path d="M10 13h7M10 17h7M10 9h3" strokeLinecap="round" />
              </svg>
            </div>
            <div className="leading-tight">
              <div className="text-sm font-semibold text-slate-100">Page the Ripper</div>
              <div className="text-xs text-slate-400">scan · OCR · serve</div>
            </div>
          </div>
          <nav className="flex flex-1 items-center gap-1">
            <NavItem to="/scan">New scan</NavItem>
            <NavItem to="/jobs">Jobs</NavItem>
            <NavItem to="/composite">Composite</NavItem>
            <NavItem to="/scanners">Scanners</NavItem>
            <NavItem to="/system">System</NavItem>
          </nav>
          <a
            href="/docs"
            target="_blank"
            rel="noreferrer"
            className="text-xs text-slate-400 hover:text-brand-400"
          >
            API docs ↗
          </a>
        </div>
      </header>

      <main className="mx-auto w-full max-w-6xl flex-1 px-4 py-6">
        <Routes>
          <Route path="/" element={<Navigate to="/scan" replace />} />
          <Route path="/scan" element={<NewScanPage />} />
          <Route path="/jobs" element={<JobsPage />} />
          <Route path="/jobs/:jobId/crop" element={<CropPage />} />
          <Route path="/composite" element={<CompositePage />} />
          <Route path="/scanners" element={<ScannersPage />} />
          <Route path="/system" element={<SystemPage />} />
          <Route path="*" element={<Navigate to="/scan" replace />} />
        </Routes>
      </main>

      <footer className="border-t border-slate-800 py-3 text-center text-xs text-slate-500">
        Page the Ripper · self-hosted scanning service
      </footer>
    </div>
  );
}
