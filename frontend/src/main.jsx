import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { ConfigProvider } from 'antd'
import 'antd/dist/reset.css'
import './index.css'
import App from './App.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <ConfigProvider
      theme={{
        token: {
          colorPrimary: '#0000E1',
          borderRadius: 4,
        },
      }}
    >
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<App />} />
          <Route path="/iam" element={<App />} />
          <Route path="/appauth" element={<App />} />
          <Route path="/platform/management" element={<App />} />
          <Route path="/application" element={<Navigate to="/application/connection" replace />} />
          <Route path="/application/connection" element={<App />} />
          <Route path="/application/connection/:connectionId" element={<App />} />
          <Route path="/application/connection/create" element={<App />} />
          <Route path="/application/transformation" element={<App />} />
          <Route path="/application/destination" element={<App />} />
          <Route path="/application/credentials" element={<App />} />
          <Route path="/apihub" element={<App />} />
          <Route path="/apihub/builder" element={<App />} />
          <Route path="/monitor" element={<App />} />
          <Route path="/settings" element={<App />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
      </BrowserRouter>
    </ConfigProvider>
  </StrictMode>,
)
