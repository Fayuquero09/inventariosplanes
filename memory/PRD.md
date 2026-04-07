# AutoConnect - Sistema de Gestión de Inventario Vehicular
## Product Requirements Document (PRD)

### Fecha de Creación: 2026-04-07

---

## 1. Problem Statement Original

Sistema web para conectar inventario de vehículos nuevos y seminuevos de diferentes agencias de diferentes grupos. Necesidades:
- Calcular costo financiero y aging de cada vehículo
- Sugerir ventas dentro de agencias del mismo grupo
- Políticas comerciales diferentes por marca/agencia
- Gestión de objetivos por línea de vehículo o revenue
- Cálculo de comisiones de vendedores
- Acceso cross-agencia dentro del mismo grupo
- Tendencias mensuales para proyección de ventas
- Importación de datos via Excel/CSV

---

## 2. User Personas

### App Admin
- Administrador total de la plataforma
- Gestiona grupos, usuarios globales

### Group Admin
- Administrador de un grupo/distribuidor
- Gestiona marcas, agencias, tasas financieras
- Ve datos consolidados del grupo

### Brand Admin
- Administrador de una marca dentro de un grupo
- Gestiona agencias de su marca

### Agency Admin
- Administrador de una agencia específica
- Gestiona inventario, vendedores, objetivos

### Seller (Vendedor)
- Registra ventas
- Ve su desempeño y comisiones

---

## 3. Core Requirements (Static)

1. **Multi-tenant Architecture**: Grupos → Marcas → Agencias
2. **Dual Authentication**: JWT + Google OAuth
3. **Role-Based Access Control (RBAC)**: 9 niveles de roles
4. **Vehicle Inventory Management**: CRUD completo con tracking de aging
5. **Financial Cost Calculation**: Tasas configurables por grupo/marca/agencia
6. **Sales Objectives**: Metas mensuales por agencia
7. **Commission Rules**: 4 tipos (por unidad, porcentaje, volumen, F&I)
8. **Smart Suggestions**: Recomendaciones basadas en históricos
9. **Data Import**: Excel/CSV para vehículos y ventas
10. **Dashboard Analytics**: KPIs, tendencias, distribución de aging

---

## 4. What's Been Implemented ✅

### Backend (FastAPI + MongoDB)
- [x] Authentication: JWT + Google OAuth endpoints
- [x] User management with 9 role types
- [x] Groups CRUD API
- [x] Brands CRUD API
- [x] Agencies CRUD API
- [x] Vehicles CRUD API with aging calculation
- [x] Financial Rates CRUD API
- [x] Sales Objectives CRUD API
- [x] Commission Rules CRUD API (4 types)
- [x] Sales CRUD API with commission calculation
- [x] Dashboard KPIs endpoint with hierarchical filters
- [x] Sales trends endpoint with hierarchical filters
- [x] Seller performance endpoint
- [x] Sellers endpoint (by agency/brand/group)
- [x] Smart suggestions endpoint
- [x] CSV/Excel import for vehicles and sales

### Frontend (React + Shadcn UI)
- [x] Login page with JWT + Google OAuth
- [x] Dashboard with hierarchical filters (Group → Brand → Agency → Seller)
- [x] Dashboard KPIs, charts, aging distribution
- [x] Seller-specific view when viewing by seller
- [x] Filter breadcrumb showing current selection
- [x] Inventory page with filters and search
- [x] Financial Rates management page
- [x] Sales Objectives page with progress tracking
- [x] Commission Rules management page
- [x] Settings page for Groups/Brands/Agencies/Users
- [x] Main layout with responsive sidebar
- [x] All data-testid attributes for testing

### Design
- [x] Swiss & High-Contrast theme (Light)
- [x] Cabinet Grotesk + IBM Plex Sans typography
- [x] Deep Blue (#002FA7) primary color
- [x] Aging badges with color coding
- [x] Professional automotive dealership aesthetic

---

## 5. Prioritized Backlog

### P0 (Critical - Next Phase)
- [ ] ERP Integration connector
- [ ] Real-time sales notifications
- [ ] Vehicle transfer between agencies
- [ ] Export reports to Excel/PDF

### P1 (Important)
- [ ] Seller mobile view optimization
- [ ] Commission payout tracking
- [ ] Historical comparison reports
- [ ] Inventory alerts (aging thresholds)

### P2 (Nice to Have)
- [ ] Multi-language support
- [ ] Dark mode theme
- [ ] Vehicle photos/gallery
- [ ] Customer management (CRM lite)

---

## 6. Architecture Summary

```
Frontend (React 19)
├── Pages: Dashboard, Inventory, FinancialRates, Objectives, Commissions, Settings
├── Components: Shadcn UI + Custom
├── State: React Context (Auth)
└── API: Axios with interceptors

Backend (FastAPI)
├── Auth: JWT + Google OAuth (Authlib)
├── Database: MongoDB (Motor async driver)
├── Models: Pydantic v2
└── Security: bcrypt, httpOnly cookies

Database (MongoDB)
├── Collections: users, groups, brands, agencies, vehicles, financial_rates, sales_objectives, commission_rules, sales
└── Indexes: email (unique), vin, agency_id, sale_date
```

---

## 7. Test Credentials

- **Admin Email:** admin@autoconnect.com
- **Admin Password:** Admin123!
- **Role:** app_admin

---

## 8. Next Tasks

1. Agregar integración con ERP específico del cliente
2. Implementar transferencia de vehículos entre agencias
3. Añadir exportación de reportes
4. Optimizar predicciones con ML básico
