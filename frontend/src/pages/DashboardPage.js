import { useState, useEffect, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { dashboardApi, vehiclesApi, agenciesApi, salesApi } from '../lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Badge } from '../components/ui/badge';
import { Button } from '../components/ui/button';
import { Skeleton } from '../components/ui/skeleton';
import {
  Car,
  CurrencyDollar,
  Clock,
  TrendUp,
  ChartBar,
  Warehouse,
  Lightning,
  CaretRight,
  ArrowUp,
  ArrowDown
} from '@phosphor-icons/react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell
} from 'recharts';

const COLORS = ['#002FA7', '#2A9D8F', '#E9C46A', '#E63946', '#8ECAE6'];

function KPICard({ title, value, icon: Icon, trend, trendUp, loading }) {
  return (
    <Card className="border-border/40" data-testid={`kpi-${title.toLowerCase().replace(/\s/g, '-')}`}>
      <CardContent className="p-4 sm:p-6">
        {loading ? (
          <div className="space-y-3">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="h-8 w-32" />
          </div>
        ) : (
          <>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold tracking-widest uppercase text-muted-foreground">
                {title}
              </span>
              <div className="w-8 h-8 rounded-md bg-[#002FA7]/10 flex items-center justify-center">
                <Icon size={18} weight="duotone" className="text-[#002FA7]" />
              </div>
            </div>
            <div className="text-2xl sm:text-3xl font-bold tracking-tight" style={{ fontFamily: 'Cabinet Grotesk' }}>
              {value}
            </div>
            {trend && (
              <div className={`flex items-center gap-1 mt-2 text-sm ${trendUp ? 'text-[#2A9D8F]' : 'text-[#E63946]'}`}>
                {trendUp ? <ArrowUp size={14} /> : <ArrowDown size={14} />}
                <span>{trend}</span>
              </div>
            )}
          </>
        )}
      </CardContent>
    </Card>
  );
}

function AgingBadge({ days }) {
  if (days <= 30) return <Badge className="aging-low">{days} días</Badge>;
  if (days <= 60) return <Badge className="aging-medium">{days} días</Badge>;
  return <Badge className="aging-high">{days} días</Badge>;
}

export default function DashboardPage() {
  const [kpis, setKpis] = useState(null);
  const [trends, setTrends] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [topVehicles, setTopVehicles] = useState([]);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    try {
      const [kpisRes, trendsRes, suggestionsRes, vehiclesRes] = await Promise.all([
        dashboardApi.getKpis(),
        dashboardApi.getTrends(),
        dashboardApi.getSuggestions(),
        vehiclesApi.getAll({ status: 'in_stock' })
      ]);

      setKpis(kpisRes.data);
      setTrends(trendsRes.data);
      setSuggestions(suggestionsRes.data.slice(0, 5));
      
      // Get top vehicles by aging
      const sortedVehicles = vehiclesRes.data
        .sort((a, b) => b.aging_days - a.aging_days)
        .slice(0, 5);
      setTopVehicles(sortedVehicles);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };

  const agingData = kpis ? [
    { name: '0-30 días', value: kpis.aging_buckets['0-30'], fill: '#2A9D8F' },
    { name: '31-60 días', value: kpis.aging_buckets['31-60'], fill: '#E9C46A' },
    { name: '61-90 días', value: kpis.aging_buckets['61-90'], fill: '#E63946' },
    { name: '90+ días', value: kpis.aging_buckets['90+'], fill: '#002FA7' }
  ] : [];

  return (
    <div className="space-y-6" data-testid="dashboard-page">
      {/* KPI Grid */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          title="Vehículos en Stock"
          value={kpis?.total_vehicles || 0}
          icon={Car}
          loading={loading}
        />
        <KPICard
          title="Valor Total Inventario"
          value={kpis ? formatCurrency(kpis.total_value) : '$0'}
          icon={CurrencyDollar}
          loading={loading}
        />
        <KPICard
          title="Costo Financiero"
          value={kpis ? formatCurrency(kpis.total_financial_cost) : '$0'}
          icon={Clock}
          loading={loading}
        />
        <KPICard
          title="Promedio Aging"
          value={`${kpis?.avg_aging_days || 0} días`}
          icon={ChartBar}
          loading={loading}
        />
      </div>

      {/* Monthly performance */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          title="Ventas del Mes"
          value={kpis?.units_sold_month || 0}
          icon={TrendUp}
          loading={loading}
        />
        <KPICard
          title="Ingresos del Mes"
          value={kpis ? formatCurrency(kpis.revenue_month) : '$0'}
          icon={CurrencyDollar}
          loading={loading}
        />
        <KPICard
          title="Vehículos Nuevos"
          value={kpis?.new_vehicles || 0}
          icon={Car}
          loading={loading}
        />
        <KPICard
          title="Vehículos Seminuevos"
          value={kpis?.used_vehicles || 0}
          icon={Car}
          loading={loading}
        />
      </div>

      {/* Charts Row */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Sales Trend Chart */}
        <Card className="border-border/40" data-testid="sales-trend-chart">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Tendencia de Ventas
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-64 w-full" />
            ) : (
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={trends}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                    <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                    <YAxis tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--card))',
                        border: '1px solid hsl(var(--border))',
                        borderRadius: '6px'
                      }}
                    />
                    <Line
                      type="monotone"
                      dataKey="units"
                      stroke="#002FA7"
                      strokeWidth={2}
                      dot={{ fill: '#002FA7' }}
                      name="Unidades"
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Aging Distribution Chart */}
        <Card className="border-border/40" data-testid="aging-distribution-chart">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Distribución de Aging
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-64 w-full" />
            ) : (
              <div className="h-64 flex items-center">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={agingData}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={90}
                      paddingAngle={2}
                      dataKey="value"
                    >
                      {agingData.map((entry, index) => (
                        <Cell key={`cell-${index}`} fill={entry.fill} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--card))',
                        border: '1px solid hsl(var(--border))',
                        borderRadius: '6px'
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
                <div className="space-y-2 ml-4">
                  {agingData.map((item, index) => (
                    <div key={index} className="flex items-center gap-2 text-sm">
                      <div className="w-3 h-3 rounded-sm" style={{ backgroundColor: item.fill }} />
                      <span className="text-muted-foreground">{item.name}:</span>
                      <span className="font-medium">{item.value}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Bottom Row */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Top Aging Vehicles */}
        <Card className="border-border/40" data-testid="top-aging-vehicles">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Vehículos con Mayor Aging
            </CardTitle>
            <Link to="/inventory">
              <Button variant="ghost" size="sm" className="text-[#002FA7]">
                Ver todos <CaretRight size={16} />
              </Button>
            </Link>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4, 5].map((i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : (
              <div className="space-y-2">
                {topVehicles.map((vehicle) => (
                  <div
                    key={vehicle.id}
                    className="flex items-center justify-between p-3 rounded-md bg-muted/30 hover:bg-muted/50 transition-fast"
                  >
                    <div>
                      <div className="font-medium">{vehicle.model} {vehicle.trim}</div>
                      <div className="text-sm text-muted-foreground">
                        {vehicle.year} • {vehicle.color} • {vehicle.agency_name}
                      </div>
                    </div>
                    <div className="text-right">
                      <AgingBadge days={vehicle.aging_days} />
                      <div className="text-sm text-muted-foreground mt-1">
                        {formatCurrency(vehicle.financial_cost)}
                      </div>
                    </div>
                  </div>
                ))}
                {topVehicles.length === 0 && (
                  <p className="text-center text-muted-foreground py-8">
                    No hay vehículos en inventario
                  </p>
                )}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Smart Suggestions */}
        <Card className="border-border/40" data-testid="smart-suggestions">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Cabinet Grotesk' }}>
              <Lightning size={20} weight="duotone" className="text-[#E9C46A]" />
              Sugerencias Inteligentes
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-20 w-full" />
                ))}
              </div>
            ) : (
              <div className="space-y-3">
                {suggestions.map((suggestion) => (
                  <div
                    key={suggestion.vehicle_id}
                    className="p-3 rounded-md border border-[#E9C46A]/30 bg-[#E9C46A]/5"
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="font-medium">
                        {suggestion.vehicle_info.model} {suggestion.vehicle_info.trim}
                      </div>
                      <Badge className="bg-[#E9C46A]/20 text-[#b89830] border-[#E9C46A]/30">
                        Bono sugerido: {formatCurrency(suggestion.suggested_bonus)}
                      </Badge>
                    </div>
                    <p className="text-sm text-muted-foreground">{suggestion.reason}</p>
                  </div>
                ))}
                {suggestions.length === 0 && (
                  <p className="text-center text-muted-foreground py-8">
                    No hay sugerencias en este momento
                  </p>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
