import { useState, useEffect, useMemo } from 'react';
import { Link, useLocation, Outlet, useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { Button } from '../components/ui/button';
import { ScrollArea } from '../components/ui/scroll-area';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '../components/ui/dropdown-menu';
import {
  Car,
  ChartBar,
  Warehouse,
  Percent,
  Target,
  CurrencyDollar,
  Gear,
  SignOut,
  List,
  X,
  CaretDown,
  User
} from '@phosphor-icons/react';

const NAV_ITEMS = [
  { path: '/', label: 'Dashboard', icon: ChartBar },
  { path: '/inventory', label: 'Inventario', icon: Warehouse },
  { path: '/financial-rates', label: 'Tasas Financieras', icon: Percent },
  { path: '/prices', label: 'Precios', icon: List },
  { path: '/objectives', label: 'Objetivos', icon: Target },
  { path: '/commissions', label: 'Comisiones', icon: CurrencyDollar },
  { path: '/settings', label: 'Configuración', icon: Gear }
];

const GROUP_FINANCE_ALLOWED_PATHS = new Set(['/', '/inventory', '/financial-rates', '/prices']);
const AGENCY_SCOPED_ROLES = new Set([
  'agency_admin',
  'agency_sales_manager',
  'agency_general_manager',
  'agency_commercial_manager',
  'agency_user',
  'seller',
]);

export default function MainLayout() {
  const { user, logout } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const isGroupFinanceManager = user?.role === 'group_finance_manager';
  const isAgencyScopedUser = AGENCY_SCOPED_ROLES.has(user?.role);
  const visibleNavItems = useMemo(() => {
    let items = NAV_ITEMS;
    if (isAgencyScopedUser) {
      items = items.filter((item) => item.path !== '/financial-rates');
    }
    if (isGroupFinanceManager) {
      items = items.filter((item) => GROUP_FINANCE_ALLOWED_PATHS.has(item.path));
    }
    return items;
  }, [isAgencyScopedUser, isGroupFinanceManager]);

  useEffect(() => {
    if (isAgencyScopedUser && location.pathname === '/financial-rates') {
      navigate('/', { replace: true });
      return;
    }
    if (!isGroupFinanceManager) return;
    if (!GROUP_FINANCE_ALLOWED_PATHS.has(location.pathname)) {
      navigate('/financial-rates', { replace: true });
    }
  }, [isAgencyScopedUser, isGroupFinanceManager, location.pathname, navigate]);

  const handleLogout = async () => {
    await logout();
  };

  return (
    <div className="min-h-screen flex bg-background">
      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <aside
        className={`fixed lg:static inset-y-0 left-0 z-50 w-64 bg-card border-r border-border/40 transform transition-transform duration-200 lg:translate-x-0 ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
        data-testid="sidebar"
      >
        <div className="flex flex-col h-full">
          {/* Logo */}
          <div className="h-16 flex items-center justify-between px-4 border-b border-border/40">
            <Link to="/" className="flex items-center gap-2">
              <div className="w-9 h-9 rounded-md bg-[#002FA7] flex items-center justify-center">
                <Car size={22} weight="duotone" className="text-white" />
              </div>
              <span className="text-lg font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
                AutoConnect
              </span>
            </Link>
            <Button
              variant="ghost"
              size="sm"
              className="lg:hidden"
              onClick={() => setSidebarOpen(false)}
            >
              <X size={20} />
            </Button>
          </div>

          {/* Navigation */}
          <ScrollArea className="flex-1 py-4">
            <nav className="px-3 space-y-1">
              {visibleNavItems.map((item) => {
                const Icon = item.icon;
                const isActive = location.pathname === item.path;
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    onClick={() => setSidebarOpen(false)}
                    className={`flex items-center gap-3 px-3 py-2.5 rounded-md text-sm font-medium transition-fast ${
                      isActive
                        ? 'bg-[#002FA7] text-white'
                        : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                    }`}
                    data-testid={`nav-${item.path.slice(1) || 'dashboard'}`}
                  >
                    <Icon size={20} weight={isActive ? 'duotone' : 'regular'} />
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </ScrollArea>

          {/* User info */}
          <div className="p-4 border-t border-border/40">
            <div className="flex items-center gap-3">
              <div className="w-9 h-9 rounded-full bg-[#002FA7]/10 flex items-center justify-center">
                <User size={18} className="text-[#002FA7]" />
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-sm font-medium truncate">{user?.name}</div>
                <div className="text-xs text-muted-foreground truncate">{user?.email}</div>
              </div>
            </div>
          </div>
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <header className="h-16 flex items-center justify-between px-4 sm:px-6 border-b border-border/40 bg-card">
          <div className="flex items-center gap-4">
            <Button
              variant="ghost"
              size="sm"
              className="lg:hidden"
              onClick={() => setSidebarOpen(true)}
              data-testid="mobile-menu-btn"
            >
              <List size={24} />
            </Button>
            <h1 className="text-lg font-semibold hidden sm:block" style={{ fontFamily: 'Cabinet Grotesk' }}>
              {visibleNavItems.find((item) => item.path === location.pathname)?.label || 'Dashboard'}
            </h1>
          </div>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="gap-2" data-testid="user-menu-btn">
                <div className="w-8 h-8 rounded-full bg-[#002FA7] flex items-center justify-center text-white text-sm font-medium">
                  {user?.name?.charAt(0).toUpperCase()}
                </div>
                <span className="hidden sm:inline">{user?.name?.split(' ')[0]}</span>
                <CaretDown size={16} className="text-muted-foreground" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56">
              <DropdownMenuLabel>
                <div>{user?.name}</div>
                <div className="text-xs font-normal text-muted-foreground">{user?.email}</div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              {!isGroupFinanceManager && (
                <>
                  <DropdownMenuItem asChild>
                    <Link to="/settings" className="cursor-pointer">
                      <Gear size={16} className="mr-2" />
                      Configuración
                    </Link>
                  </DropdownMenuItem>
                  <DropdownMenuSeparator />
                </>
              )}
              <DropdownMenuItem onClick={handleLogout} className="text-destructive cursor-pointer" data-testid="logout-btn">
                <SignOut size={16} className="mr-2" />
                Cerrar Sesión
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </header>

        {/* Page content */}
        <main className="flex-1 p-4 sm:p-6 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
