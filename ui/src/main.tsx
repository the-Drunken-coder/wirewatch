import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@blueprintjs/core/lib/css/blueprint.css';
import './style.css';
import { App } from './App';
import { installGlobalReporting, reportRender } from './report';

installGlobalReporting();
const root = document.getElementById('root');
if (!root) throw new Error('Missing #root element.');
createRoot(root, { onUncaughtError: reportRender, onCaughtError: reportRender, onRecoverableError: reportRender })
  .render(<StrictMode><App /></StrictMode>);
