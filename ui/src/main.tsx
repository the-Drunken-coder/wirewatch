import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import '@blueprintjs/core/lib/css/blueprint.css';
import './style.css';
import { App } from './App';

const root = document.getElementById('root');
if (!root) throw new Error('Missing #root element.');
createRoot(root).render(<StrictMode><App /></StrictMode>);
