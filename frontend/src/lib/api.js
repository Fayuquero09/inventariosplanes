import axios from 'axios';

const API_URL = process.env.REACT_APP_BACKEND_URL;

const api = axios.create({
  baseURL: `${API_URL}/api`,
  withCredentials: true
});

// Add token to all requests
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Handle 401 errors
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('token');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);

// Auth
export const authApi = {
  login: (data) => api.post('/auth/login', data),
  register: (data) => api.post('/auth/register', data),
  logout: () => api.post('/auth/logout'),
  me: () => api.get('/auth/me'),
  resetPassword: (email, newPassword) => api.post('/auth/reset-password', { email, new_password: newPassword }),
  googleAuth: (credential) => api.post('/auth/google', { credential })
};

// Groups
export const groupsApi = {
  getAll: () => api.get('/groups'),
  getOne: (id) => api.get(`/groups/${id}`),
  create: (data) => api.post('/groups', data),
  update: (id, data) => api.put(`/groups/${id}`, data),
  delete: (id, params = {}) => api.delete(`/groups/${id}`, { params })
};

// Brands
export const brandsApi = {
  getAll: (groupId) => api.get('/brands', { params: { group_id: groupId } }),
  create: (data) => api.post('/brands', data),
  update: (id, data) => api.put(`/brands/${id}`, data),
  delete: (id, params = {}) => api.delete(`/brands/${id}`, { params })
};

// Agencies
export const agenciesApi = {
  getAll: (params) => api.get('/agencies', { params }),
  create: (data) => api.post('/agencies', data),
  update: (id, data) => api.put(`/agencies/${id}`, data)
};

// Financial Rates
export const financialRatesApi = {
  getAll: (params) => api.get('/financial-rates', { params }),
  create: (data) => api.post('/financial-rates', data),
  update: (id, data) => api.put(`/financial-rates/${id}`, data),
  delete: (id) => api.delete(`/financial-rates/${id}`),
  applyGroupDefault: (data) => api.post('/financial-rates/apply-group-default', data)
};

// Vehicles
export const vehiclesApi = {
  getAll: (params) => api.get('/vehicles', { params }),
  getOne: (id) => api.get(`/vehicles/${id}`),
  create: (data) => api.post('/vehicles', data),
  update: (id, data) => api.put(`/vehicles/${id}`, data),
  import: (file) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/import/vehicles', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
  }
};

// Vehicle Catalog (Strapi/JATO, model year constrained by backend)
export const vehicleCatalogApi = {
  getMakes: () => api.get('/catalog/makes'),
  getModels: (make, options = {}) => api.get('/catalog/models', {
    params: {
      make,
      ...(options?.allYears ? { all_years: true } : {}),
    }
  }),
  getVersions: (make, model) => api.get('/catalog/versions', { params: { make, model } })
};

// Sales Objectives
export const salesObjectivesApi = {
  getAll: (params) => api.get('/sales-objectives', { params }),
  suggest: (params) => api.get('/sales-objectives/suggestion', { params }),
  create: (data) => api.post('/sales-objectives', data),
  update: (id, data) => api.put(`/sales-objectives/${id}`, data),
  approve: (id, data) => api.post(`/sales-objectives/${id}/approval`, data)
};

// Commission Rules
export const commissionRulesApi = {
  getAll: (params) => api.get('/commission-rules', { params }),
  create: (data) => api.post('/commission-rules', data),
  update: (id, data) => api.put(`/commission-rules/${id}`, data),
  approve: (id, data) => api.post(`/commission-rules/${id}/approval`, data),
  delete: (id) => api.delete(`/commission-rules/${id}`)
};

export const commissionClosuresApi = {
  getAll: (params) => api.get('/commission-closures', { params }),
  create: (data) => api.post('/commission-closures', data),
  approve: (id, data) => api.post(`/commission-closures/${id}/approval`, data)
};

export const commissionSimulatorApi = {
  simulate: (data) => api.post('/commission-simulator', data)
};

// Sales
export const salesApi = {
  getAll: (params) => api.get('/sales', { params }),
  create: (data) => api.post('/sales', data),
  import: (file) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/import/sales', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
  }
};

// Users
export const usersApi = {
  getAll: () => api.get('/users'),
  update: (id, data) => api.put(`/users/${id}`, data),
  delete: (id) => api.delete(`/users/${id}`)
};

// Audit Logs
export const auditLogsApi = {
  getAll: (params) => api.get('/audit-logs', { params })
};

// Sellers
export const sellersApi = {
  getAll: (params) => api.get('/sellers', { params })
};

// Organization Imports (groups, brands, agencies, sellers)
export const organizationImportApi = {
  import: (file) => {
    const formData = new FormData();
    formData.append('file', file);
    return api.post('/import/organization', formData, {
      headers: { 'Content-Type': 'multipart/form-data' }
    });
  }
};

// Dashboard
export const dashboardApi = {
  getKpis: (params) => api.get('/dashboard/kpis', { params }),
  getTrends: (params) => api.get('/dashboard/trends', { params }),
  getSellerPerformance: (params) => api.get('/dashboard/seller-performance', { params }),
  getSuggestions: (params) => api.get('/dashboard/suggestions', { params }),
  getMonthlyClose: (params) => api.get('/dashboard/monthly-close', { params }),
  getMonthlyCloseCalendar: (params) => api.get('/dashboard/monthly-close-calendar', { params }),
  upsertMonthlyClose: (data) => api.put('/dashboard/monthly-close', data)
};

export default api;
