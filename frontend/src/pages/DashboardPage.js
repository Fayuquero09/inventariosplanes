import { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { Link } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { dashboardApi, vehiclesApi, groupsApi, brandsApi, agenciesApi, sellersApi } from '../lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import SafeChart from '../components/SafeChart';
import { Badge } from '../components/ui/badge';
import { Button } from '../components/ui/button';
import { Skeleton } from '../components/ui/skeleton';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../components/ui/select';
import {
  Car,
  CurrencyDollar,
  Clock,
  TrendUp,
  Lightning,
  CaretRight,
  ArrowUp,
  ArrowDown,
  Buildings,
  Factory,
  Storefront,
  User
} from '@phosphor-icons/react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  LineChart,
  Line,
  ReferenceLine,
} from 'recharts';

function pad2(value) {
  return String(value).padStart(2, '0');
}

function getNthWeekdayOfMonth(year, month, weekday, occurrence) {
  const first = new Date(year, month - 1, 1);
  const firstWeekday = first.getDay();
  return 1 + ((weekday - firstWeekday + 7) % 7) + ((occurrence - 1) * 7);
}

function getMexicoHolidaySet(year) {
  const holidays = new Set();
  const addHoliday = (month, day) => holidays.add(`${year}-${pad2(month)}-${pad2(day)}`);

  // LFT (Art. 74): feriados federales base para operación comercial.
  addHoliday(1, 1);   // 1 enero
  addHoliday(2, getNthWeekdayOfMonth(year, 2, 1, 1));   // 1er lunes febrero
  addHoliday(3, getNthWeekdayOfMonth(year, 3, 1, 3));   // 3er lunes marzo
  addHoliday(5, 1);   // 1 mayo
  addHoliday(9, 16);  // 16 septiembre
  addHoliday(11, getNthWeekdayOfMonth(year, 11, 1, 3)); // 3er lunes noviembre
  addHoliday(12, 25); // 25 diciembre

  // Transmisión del Poder Ejecutivo Federal: 1/oct cada 6 años (2024, 2030, ...).
  if (year >= 2024 && ((year - 2024) % 6 === 0)) {
    addHoliday(10, 1);
  }

  return holidays;
}

function computeIndustryEffectiveDate(year, month, closeDay, monthOffset = 0) {
  const dayNumber = Number(closeDay);
  const normalizedOffset = Number(monthOffset);
  if (!Number.isFinite(dayNumber) || !Number.isInteger(dayNumber) || dayNumber < 1) {
    return null;
  }
  if (!Number.isFinite(normalizedOffset) || !Number.isInteger(normalizedOffset) || normalizedOffset < 0 || normalizedOffset > 1) {
    return null;
  }

  const baseMonthDate = new Date(Date.UTC(year, (month - 1) + normalizedOffset, 1, 12, 0, 0));
  const baseYear = baseMonthDate.getUTCFullYear();
  const baseMonth = baseMonthDate.getUTCMonth() + 1;
  const maxDay = new Date(baseYear, baseMonth, 0).getDate();
  if (dayNumber > maxDay) {
    return null;
  }

  const initial = new Date(Date.UTC(baseYear, baseMonth - 1, dayNumber, 12, 0, 0));
  const date = new Date(initial);

  while (true) {
    const y = date.getUTCFullYear();
    const m = date.getUTCMonth() + 1;
    const d = date.getUTCDate();
    const weekday = date.getUTCDay(); // 0 domingo
    const holidaysSet = getMexicoHolidaySet(y);
    const dateKey = `${y}-${pad2(m)}-${pad2(d)}`;
    const isSunday = weekday === 0;
    const isHoliday = holidaysSet.has(dateKey);
    if (!isSunday && !isHoliday) {
      const shifted = y !== baseYear || m !== baseMonth || d !== dayNumber;
      return {
        year: y,
        month: m,
        day: d,
        baseYear,
        baseMonth,
        shifted,
      };
    }
    date.setUTCDate(date.getUTCDate() + 1);
  }
}

function KPICard({
  title,
  value,
  icon: Icon,
  trend,
  trendUp,
  loading,
  subtitle,
  secondarySubtitle,
  badge,
  statusLabel,
  statusColor
}) {
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
            {subtitle && (
              <div className="text-sm text-muted-foreground mt-1">{subtitle}</div>
            )}
            {secondarySubtitle && (
              <div className="text-sm text-muted-foreground">{secondarySubtitle}</div>
            )}
            {badge && (
              <div className="mt-2">
                <Badge className="bg-[#002FA7]/10 text-[#002FA7] border-[#002FA7]/20">
                  {badge}
                </Badge>
              </div>
            )}
            {statusLabel && (
              <div className="mt-2 inline-flex items-center gap-2 text-xs font-medium text-muted-foreground">
                <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: statusColor || '#9CA3AF' }} />
                <span>{statusLabel}</span>
              </div>
            )}
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

function FilterBreadcrumb({ group, brand, agency, seller }) {
  const parts = [];
  if (group) parts.push({ icon: Buildings, label: group.name });
  if (brand) parts.push({ icon: Factory, label: brand.name });
  if (agency) parts.push({ icon: Storefront, label: agency.name });
  if (seller) parts.push({ icon: User, label: seller.name });

  if (parts.length === 0) return null;

  return (
    <div className="flex items-center gap-2 text-sm text-muted-foreground mb-4">
      <span>Viendo:</span>
      {parts.map((part, index) => {
        const Icon = part.icon;
        return (
          <span key={index} className="flex items-center gap-1">
            {index > 0 && <CaretRight size={12} />}
            <Icon size={14} />
            <span className="font-medium text-foreground">{part.label}</span>
          </span>
        );
      })}
    </div>
  );
}

export default function DashboardPage() {
  const { user } = useAuth();
  const [kpis, setKpis] = useState(null);
  const [trends, setTrends] = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [topVehicles, setTopVehicles] = useState([]);
  const [brandVehicles, setBrandVehicles] = useState([]);
  const [loading, setLoading] = useState(true);
  const latestRequestRef = useRef(0);

  // Filter data
  const [groups, setGroups] = useState([]);
  const [brands, setBrands] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [sellers, setSellers] = useState([]);

  // Selected filters
  const [selectedGroup, setSelectedGroup] = useState('all');
  const [selectedBrand, setSelectedBrand] = useState('all');
  const [selectedAgency, setSelectedAgency] = useState('all');
  const [selectedSeller, setSelectedSeller] = useState('all');
  const [monthlyCloseCalendar, setMonthlyCloseCalendar] = useState([]);

  // Check if user is super admin/user (can see all groups)
  const isSuperUser = user?.role === 'app_admin' || user?.role === 'app_user';
  const canSelectGroup = isSuperUser && groups.length > 1;

  // Load filter options
  useEffect(() => {
    const loadFilterOptions = async () => {
      try {
        const [groupsRes, brandsRes, agenciesRes] = await Promise.all([
          groupsApi.getAll(),
          brandsApi.getAll(),
          agenciesApi.getAll()
        ]);
        setGroups(groupsRes.data);
        setBrands(brandsRes.data);
        setAgencies(agenciesRes.data);
        
        // Si el usuario no es super user y solo tiene un grupo, seleccionarlo automáticamente
        if (!isSuperUser && groupsRes.data.length === 1) {
          setSelectedGroup(groupsRes.data[0].id);
        }
      } catch (error) {
        console.error('Error loading filter options:', error);
      }
    };
    loadFilterOptions();
  }, [isSuperUser]);

  // Load sellers when agency changes
  useEffect(() => {
    const loadSellers = async () => {
      if (selectedAgency !== 'all') {
        try {
          const res = await sellersApi.getAll({ agency_id: selectedAgency });
          setSellers(res.data);
        } catch (error) {
          console.error('Error loading sellers:', error);
          setSellers([]);
        }
      } else {
        setSellers([]);
        setSelectedSeller('all');
      }
    };
    loadSellers();
  }, [selectedAgency]);

  const handleGroupChange = useCallback((value) => {
    setSelectedGroup(value);
    setSelectedBrand('all');
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, []);

  const handleBrandChange = useCallback((value) => {
    setSelectedBrand(value);
    setSelectedAgency('all');
    setSelectedSeller('all');
  }, []);

  const handleAgencyChange = useCallback((value) => {
    setSelectedAgency(value);
    setSelectedSeller('all');
  }, []);

  const scopeParams = useMemo(() => {
    const params = {};
    if (selectedGroup !== 'all') params.group_id = selectedGroup;
    if (selectedBrand !== 'all') params.brand_id = selectedBrand;
    if (selectedAgency !== 'all') params.agency_id = selectedAgency;
    if (selectedSeller !== 'all') params.seller_id = selectedSeller;
    return params;
  }, [selectedGroup, selectedBrand, selectedAgency, selectedSeller]);

  const hasBrandContext = useMemo(
    () => (
      selectedBrand !== 'all'
      || selectedAgency !== 'all'
      || Boolean(user?.brand_id)
      || Boolean(user?.agency_id)
    ),
    [selectedBrand, selectedAgency, user?.brand_id, user?.agency_id],
  );

  // Fetch dashboard data
  const fetchData = useCallback(async () => {
    const requestId = latestRequestRef.current + 1;
    latestRequestRef.current = requestId;
    setLoading(true);
    try {
      const params = { ...scopeParams };

      const isSellerScope = selectedSeller !== 'all';
      const trendParams = isSellerScope
        ? { ...params, months: 6 }
        : { ...params, months: 1, granularity: 'day' };

      const [kpisRes, trendsRes] = await Promise.all([
        dashboardApi.getKpis(params),
        dashboardApi.getTrends(trendParams),
      ]);

      if (requestId !== latestRequestRef.current) return;
      setKpis(kpisRes.data);
      setTrends(trendsRes.data || []);

      if (hasBrandContext) {
        const [suggestionsRes, vehiclesRes] = await Promise.all([
          dashboardApi.getSuggestions({
            group_id: params.group_id,
            brand_id: params.brand_id,
            agency_id: params.agency_id,
          }),
          vehiclesApi.getAll({
            status: 'in_stock',
            group_id: params.group_id,
            brand_id: params.brand_id,
            agency_id: params.agency_id,
          }),
        ]);

        if (requestId !== latestRequestRef.current) return;
        setSuggestions(suggestionsRes.data.slice(0, 5));

        // Get top vehicles by aging
        const sortedVehicles = vehiclesRes.data
          .sort((a, b) => b.aging_days - a.aging_days)
        setBrandVehicles(sortedVehicles);
        setTopVehicles(sortedVehicles.slice(0, 5));
      } else {
        setSuggestions([]);
        setTopVehicles([]);
        setBrandVehicles([]);
      }
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      setSuggestions([]);
      setTopVehicles([]);
      setBrandVehicles([]);
    } finally {
      if (requestId === latestRequestRef.current) {
        setLoading(false);
      }
    }
  }, [hasBrandContext, scopeParams, selectedSeller]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const fetchMonthlyCloseCalendar = useCallback(async () => {
    try {
      const currentYear = new Date().getFullYear();
      const res = await dashboardApi.getMonthlyCloseCalendar({
        year: currentYear,
        from_current_month: true,
      });
      const items = Array.isArray(res?.data?.items) ? res.data.items : [];
      setMonthlyCloseCalendar(items);
    } catch (error) {
      console.error('Error loading monthly close calendar:', error);
      setMonthlyCloseCalendar([]);
    }
  }, []);

  useEffect(() => {
    fetchMonthlyCloseCalendar();
  }, [fetchMonthlyCloseCalendar]);

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };

  const formatUnits = (value) => {
    return new Intl.NumberFormat('es-MX', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(Number(value || 0));
  };

  const formatPercent = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return 'N/D';
    }
    const numeric = Number(value);
    const sign = numeric > 0 ? '+' : '';
    return `${sign}${numeric.toFixed(1)}%`;
  };

  // Filter brands by selected group
  const filteredBrands = selectedGroup !== 'all' 
    ? brands.filter(b => b.group_id === selectedGroup)
    : brands;

  // Filter agencies by selected brand (or group)
  const filteredAgencies = selectedBrand !== 'all'
    ? agencies.filter(a => a.brand_id === selectedBrand)
    : selectedGroup !== 'all'
      ? agencies.filter(a => a.group_id === selectedGroup)
      : agencies;

  // Get selected entities for breadcrumb
  const selectedGroupObj = groups.find(g => g.id === selectedGroup);
  const selectedBrandObj = brands.find(b => b.id === selectedBrand);
  const selectedAgencyObj = agencies.find(a => a.id === selectedAgency);
  const selectedSellerObj = sellers.find(s => s.id === selectedSeller);

  const agingData = kpis ? [
    { name: '0-30 días', value: kpis.aging_buckets['0-30'], fill: '#2A9D8F' },
    { name: '31-60 días', value: kpis.aging_buckets['31-60'], fill: '#E9C46A' },
    { name: '61-90 días', value: kpis.aging_buckets['61-90'], fill: '#E63946' },
    { name: '90+ días', value: kpis.aging_buckets['90+'], fill: '#002FA7' }
  ] : [];

  // Check if viewing seller level
  const isSellerView = selectedSeller !== 'all';
  const currentMonthKey = useMemo(() => {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
  }, []);

  const currentMonthTrendData = useMemo(() => {
    const exactMonth = trends.filter((point) => point?.month === currentMonthKey);
    if (exactMonth.length > 0) return exactMonth;
    if (trends.length > 0) return [trends[trends.length - 1]];
    return [];
  }, [trends, currentMonthKey]);

  const latestElapsedTrendPoint = useMemo(() => {
    if (!currentMonthTrendData.length) return null;
    const reversed = [...currentMonthTrendData].reverse();
    return (
      reversed.find((point) => point?.is_elapsed_day !== false && point?.units !== null && point?.units !== undefined)
      || reversed.find((point) => point?.units !== null && point?.units !== undefined)
      || currentMonthTrendData[currentMonthTrendData.length - 1]
    );
  }, [currentMonthTrendData]);

  const hasObjectiveData = currentMonthTrendData.some((point) => Number(point?.objective_units || 0) > 0);
  const hasAgingData = agingData.some((item) => Number(item.value || 0) > 0);
  const salesTrendXAxisKey = currentMonthTrendData.some((point) => point?.day_label) ? 'day_label' : 'month';
  const salesCalendarLabel = useMemo(() => {
    let year = Number(currentMonthKey.split('-')[0]);
    let month = Number(currentMonthKey.split('-')[1]);
    if (currentMonthTrendData.length > 0 && currentMonthTrendData[0]?.month) {
      const [trendYear, trendMonth] = String(currentMonthTrendData[0].month).split('-').map(Number);
      if (Number.isFinite(trendYear) && Number.isFinite(trendMonth)) {
        year = trendYear;
        month = trendMonth;
      }
    }
    const monthEndDay = new Date(year, month, 0).getDate();
    const monthName = new Date(year, month - 1, 1).toLocaleDateString('es-MX', { month: 'long' });
    return `Calendario: 1 al ${monthEndDay} de ${monthName} ${year}`;
  }, [currentMonthKey, currentMonthTrendData]);

  const salesCalendarScope = useMemo(() => {
    let year = Number(currentMonthKey.split('-')[0]);
    let month = Number(currentMonthKey.split('-')[1]);
    if (currentMonthTrendData.length > 0 && currentMonthTrendData[0]?.month) {
      const [trendYear, trendMonth] = String(currentMonthTrendData[0].month).split('-').map(Number);
      if (Number.isFinite(trendYear) && Number.isFinite(trendMonth)) {
        year = trendYear;
        month = trendMonth;
      }
    }
    return {
      year,
      month,
      holidays: getMexicoHolidaySet(year),
    };
  }, [currentMonthKey, currentMonthTrendData]);

  const salesXAxisTick = useMemo(() => {
    if (salesTrendXAxisKey !== 'day_label') {
      return { fontSize: 12 };
    }

    return ({ x, y, payload }) => {
      const label = String(payload?.value ?? '');
      const day = Number(label);
      let fillColor = 'hsl(var(--muted-foreground))';

      if (Number.isFinite(day) && day > 0) {
        const dateKey = `${salesCalendarScope.year}-${pad2(salesCalendarScope.month)}-${pad2(day)}`;
        const weekDay = new Date(salesCalendarScope.year, salesCalendarScope.month - 1, day).getDay();
        const isSunday = weekDay === 0;
        const isSaturday = weekDay === 6;
        const isHoliday = salesCalendarScope.holidays.has(dateKey);

        if (isSunday || isHoliday) {
          fillColor = '#E63946'; // rojo
        } else if (isSaturday) {
          fillColor = '#C28A00'; // amarillo (tono legible)
        } else {
          fillColor = '#2A9D8F'; // verde
        }
      }

      return (
        <text x={x} y={y + 12} textAnchor="middle" fill={fillColor} fontSize={12}>
          {label}
        </text>
      );
    };
  }, [salesTrendXAxisKey, salesCalendarScope]);

  const agingGaussianData = useMemo(() => {
    if (!kpis?.aging_buckets) return [];
    const b0 = Number(kpis.aging_buckets['0-30'] || 0);
    const b1 = Number(kpis.aging_buckets['31-60'] || 0);
    const b2 = Number(kpis.aging_buckets['61-90'] || 0);
    const b3 = Number(kpis.aging_buckets['90+'] || 0);
    const total = b0 + b1 + b2 + b3;
    if (total <= 0) return [];

    const baseKnownSum = (b0 * 15) + (b1 * 45) + (b2 * 75);
    const estimatedOpenBucketCenter = b3 > 0
      ? Math.max(95, Math.min(240, ((Number(kpis.avg_aging_days || 0) * total) - baseKnownSum) / b3))
      : 120;

    const weightedPoints = [
      { x: 15, w: b0 },
      { x: 45, w: b1 },
      { x: 75, w: b2 },
      { x: estimatedOpenBucketCenter, w: b3 }
    ].filter((point) => point.w > 0);

    if (!weightedPoints.length) return [];

    const weightedSum = weightedPoints.reduce((acc, point) => acc + (point.x * point.w), 0);
    const mean = weightedSum / total;
    const variance = weightedPoints.reduce((acc, point) => acc + (point.w * ((point.x - mean) ** 2)), 0) / total;
    const sigma = Math.max(Math.sqrt(variance), 8);
    const maxBucketCount = Math.max(b0, b1, b2, b3, 1);
    const maxX = Math.ceil(Math.max(180, mean + (sigma * 3), estimatedOpenBucketCenter + 40) / 5) * 5;

    const raw = [];
    for (let x = 0; x <= maxX; x += 5) {
      const z = (x - mean) / sigma;
      const density = Math.exp(-0.5 * z * z) / (sigma * Math.sqrt(2 * Math.PI));
      raw.push({ aging_days: x, density });
    }
    const maxDensity = Math.max(...raw.map((point) => point.density), 0.000001);

    return raw.map((point) => ({
      aging_days: point.aging_days,
      gauss_value: Number(((point.density / maxDensity) * maxBucketCount).toFixed(2))
    }));
  }, [kpis]);

  const currentMonthSummary = useMemo(() => {
    if (!latestElapsedTrendPoint) {
      return {
        units: 0,
        objectiveUnits: 0,
        objectiveGapPct: null,
      };
    }

    const point = latestElapsedTrendPoint || {};
    const units = Number(point?.units || 0);
    const objectiveUnits = Number(point?.weighted_objective_units || 0);
    const objectiveGapPct = objectiveUnits > 0
      ? ((units - objectiveUnits) / objectiveUnits) * 100
      : null;

    return {
      units,
      objectiveUnits,
      objectiveGapPct,
    };
  }, [latestElapsedTrendPoint]);

  const objectiveSource = useMemo(() => {
    if (!currentMonthTrendData.length) return 'none';
    const sourcePoint = latestElapsedTrendPoint || currentMonthTrendData[currentMonthTrendData.length - 1];
    return sourcePoint?.objective_source || 'none';
  }, [currentMonthTrendData, latestElapsedTrendPoint]);

  const objectiveLegendName = objectiveSource === 'benchmark_last_year'
    ? 'Objetivo ponderado (benchmark)'
    : 'Objetivo ponderado';

  const monthlyObjectiveUnits = useMemo(() => {
    const sourcePoint = latestElapsedTrendPoint || currentMonthTrendData[currentMonthTrendData.length - 1];
    return Number(sourcePoint?.objective_units || 0);
  }, [latestElapsedTrendPoint, currentMonthTrendData]);

  const projectedMonthUnits = useMemo(() => {
    if (!currentMonthTrendData.length) {
      return Number(kpis?.units_sold_month || 0);
    }
    const monthEndForecastPoint = [...currentMonthTrendData]
      .reverse()
      .find((point) => point?.forecast_units !== null && point?.forecast_units !== undefined);
    return Number(monthEndForecastPoint?.forecast_units || 0);
  }, [currentMonthTrendData, kpis]);

  const salesObjectiveLabel = monthlyObjectiveUnits > 0
    ? `Objetivo mes: ${formatUnits(monthlyObjectiveUnits)} unidades`
    : 'Objetivo mes: N/D';

  const salesForecastLabel = `Pronóstico cierre: ${formatUnits(projectedMonthUnits)} unidades`;

  const salesObjectiveSignal = useMemo(() => {
    if (monthlyObjectiveUnits <= 0) {
      return { label: 'Semáforo: sin objetivo', color: '#9CA3AF' };
    }
    const ratio = projectedMonthUnits / monthlyObjectiveUnits;
    if (ratio >= 1) {
      return { label: 'Semáforo: verde (en ruta)', color: '#2A9D8F' };
    }
    if (ratio >= 0.9) {
      return { label: 'Semáforo: amarillo (cerca)', color: '#E9C46A' };
    }
    return { label: 'Semáforo: rojo (riesgo)', color: '#E63946' };
  }, [monthlyObjectiveUnits, projectedMonthUnits]);

  const sellerBenchmarkAverageLabel = useMemo(() => {
    const benchmark = Number(kpis?.benchmark_avg_units_per_seller_month || 0);
    if (!benchmark) {
      return 'Benchmark prom. ventas/vendedor: N/D';
    }
    return `Benchmark prom. ventas/vendedor: ${benchmark.toFixed(1)}`;
  }, [kpis]);

  const sellerBenchmarkDeltaLabel = useMemo(() => {
    const deltaPct = kpis?.avg_units_per_seller_vs_benchmark_pct;
    if (deltaPct === null || deltaPct === undefined) {
      return null;
    }
    return `Brecha vs benchmark: ${formatPercent(deltaPct)}`;
  }, [kpis]);

  const sellerChallengeBadge = useMemo(() => {
    if (!kpis?.seller_challenge_tier || kpis.seller_challenge_tier === 'Sin benchmark') {
      return 'Desafío: Define benchmark';
    }
    return `Desafío ${kpis.seller_challenge_tier}`;
  }, [kpis]);

  const currentMonthCloseConfig = useMemo(() => {
    return monthlyCloseCalendar.find(
      (item) => Number(item?.year) === Number(salesCalendarScope.year)
        && Number(item?.month) === Number(salesCalendarScope.month)
    ) || null;
  }, [monthlyCloseCalendar, salesCalendarScope]);

  const currentFiscalCloseDay = useMemo(() => {
    const day = Number(currentMonthCloseConfig?.fiscal_close_day);
    return Number.isFinite(day) && day > 0 ? day : null;
  }, [currentMonthCloseConfig]);

  const currentIndustryOperationalMarker = useMemo(() => {
    const currentYear = Number(salesCalendarScope.year);
    const currentMonth = Number(salesCalendarScope.month);
    const candidates = monthlyCloseCalendar
      .map((item) => {
        const scheduledDay = Number(item?.industry_close_day);
        const monthOffset = Number(item?.industry_close_month_offset ?? 0);
        const effective = computeIndustryEffectiveDate(
          Number(item?.year),
          Number(item?.month),
          scheduledDay,
          monthOffset
        );
        if (!effective) return null;
        return {
          sourceYear: Number(item?.year),
          sourceMonth: Number(item?.month),
          scheduledDay,
          monthOffset,
          effective,
        };
      })
      .filter(Boolean)
      .filter((row) => row.effective.year === currentYear && row.effective.month === currentMonth);

    if (!candidates.length) return null;
    const shiftedFromPrevious = candidates.find(
      (row) => row.sourceYear !== currentYear || row.sourceMonth !== currentMonth
    );
    return shiftedFromPrevious || candidates[0];
  }, [monthlyCloseCalendar, salesCalendarScope]);

  const fiscalCloseXAxisValue = currentFiscalCloseDay ? pad2(currentFiscalCloseDay) : null;
  const industryCloseXAxisValue = currentIndustryOperationalMarker?.effective?.day
    ? pad2(currentIndustryOperationalMarker.effective.day)
    : null;
  const industryCloseXAxisLabel = currentIndustryOperationalMarker?.effective?.shifted
    ? 'Cierre industria (traslado)'
    : 'Cierre industria';

  return (
    <div className="space-y-6" data-testid="dashboard-page">
      {/* Filters */}
      <Card className="border-border/40">
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-3">
            {/* Group Filter - Solo visible para super users con múltiples grupos */}
            {canSelectGroup && (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <Buildings size={12} /> Grupo
                </label>
                <Select value={selectedGroup} onValueChange={handleGroupChange}>
                  <SelectTrigger className="w-[180px]" data-testid="filter-group">
                    <SelectValue placeholder="Todos los grupos" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todos los grupos</SelectItem>
                    {groups.map((group) => (
                      <SelectItem key={group.id} value={group.id}>
                        {group.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}

            {/* Si no es super user, mostrar el nombre del grupo */}
            {!canSelectGroup && groups.length === 1 && (
              <div className="flex flex-col gap-1">
                <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                  <Buildings size={12} /> Grupo
                </label>
                <div className="h-10 px-3 py-2 rounded-md border border-border bg-muted/30 flex items-center text-sm font-medium">
                  {groups[0]?.name}
                </div>
              </div>
            )}

            {/* Brand Filter */}
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <Factory size={12} /> Marca
              </label>
              <Select 
                value={selectedBrand} 
                onValueChange={handleBrandChange}
                disabled={canSelectGroup && selectedGroup === 'all'}
              >
                <SelectTrigger className="w-[180px]" data-testid="filter-brand">
                  <SelectValue placeholder="Todas las marcas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todas las marcas</SelectItem>
                  {filteredBrands.map((brand) => (
                    <SelectItem key={brand.id} value={brand.id}>
                      {brand.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Agency Filter */}
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <Storefront size={12} /> Agencia
              </label>
              <Select 
                value={selectedAgency} 
                onValueChange={handleAgencyChange}
                disabled={selectedBrand === 'all'}
              >
                <SelectTrigger className="w-[180px]" data-testid="filter-agency">
                  <SelectValue placeholder="Todas las agencias" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todas las agencias</SelectItem>
                  {filteredAgencies.map((agency) => (
                    <SelectItem key={agency.id} value={agency.id}>
                      {agency.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Seller Filter */}
            <div className="flex flex-col gap-1">
              <label className="text-xs font-medium text-muted-foreground flex items-center gap-1">
                <User size={12} /> Vendedor
              </label>
              <Select 
                value={selectedSeller} 
                onValueChange={setSelectedSeller}
                disabled={selectedAgency === 'all'}
              >
                <SelectTrigger className="w-[180px]" data-testid="filter-seller">
                  <SelectValue placeholder="Todos los vendedores" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todos los vendedores</SelectItem>
                  {sellers.map((seller) => (
                    <SelectItem key={seller.id} value={seller.id}>
                      {seller.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Breadcrumb */}
      <FilterBreadcrumb 
        group={selectedGroupObj}
        brand={selectedBrandObj}
        agency={selectedAgencyObj}
        seller={selectedSellerObj}
      />

      {/* KPI Grid - Inventory (not shown for seller view) */}
      {!isSellerView && (
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
          title="Costo Financiero Mes"
          value={kpis ? formatCurrency(kpis.total_financial_cost) : '$0'}
          icon={Clock}
          loading={loading}
        />
          <KPICard
            title="Vendedores Totales"
            value={kpis?.seller_count || 0}
            icon={User}
            subtitle={`Promedio ventas/vendedor: ${Number(kpis?.avg_units_per_seller_month || 0).toFixed(1)}`}
            secondarySubtitle={sellerBenchmarkAverageLabel}
            badge={sellerChallengeBadge}
            loading={loading}
          />
        </div>
      )}

      {/* KPI Grid - Sales Performance */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
        <KPICard
          title="Ventas del Mes"
          value={kpis?.units_sold_month || 0}
          icon={TrendUp}
          subtitle={salesObjectiveLabel}
          secondarySubtitle={salesForecastLabel}
          statusLabel={salesObjectiveSignal.label}
          statusColor={salesObjectiveSignal.color}
          loading={loading}
        />
        <KPICard
          title="Ingresos del Mes"
          value={kpis ? formatCurrency(kpis.revenue_month) : '$0'}
          icon={CurrencyDollar}
          loading={loading}
        />
        <KPICard
          title="Comisiones del Mes"
          value={kpis ? formatCurrency(kpis.commissions_month || 0) : '$0'}
          icon={CurrencyDollar}
          loading={loading}
        />
        <KPICard
          title="Utilidad Bruta Mes"
          value={kpis ? formatCurrency(kpis.gross_profit_month || 0) : '$0'}
          icon={CurrencyDollar}
          subtitle={kpis ? `Margen: ${formatPercent(kpis.gross_margin_pct_month || 0)}` : ''}
          loading={loading}
        />
        {!isSellerView ? (
          <KPICard
            title="Vehículos Nuevos / Seminuevos"
            value={`${kpis?.new_vehicles || 0} / ${kpis?.used_vehicles || 0}`}
            icon={Car}
            loading={loading}
          />
        ) : (
          <KPICard
            title="Promedio por Venta"
            value={kpis && kpis.units_sold_month > 0 
              ? formatCurrency(kpis.revenue_month / kpis.units_sold_month) 
              : '$0'}
            icon={CurrencyDollar}
            loading={loading}
          />
        )}
      </div>

      {!isSellerView && (
        <Card className="border-border/40">
          <CardContent className="p-4 flex flex-wrap items-center gap-2">
            <Badge className="bg-[#E9C46A]/15 text-[#9b7a1b] border-[#E9C46A]/40">
              {sellerChallengeBadge}
            </Badge>
            <Badge className="bg-[#002FA7]/10 text-[#002FA7] border-[#002FA7]/20">
              Promedio actual: {Number(kpis?.avg_units_per_seller_month || 0).toFixed(1)} unidades/vendedor
            </Badge>
            <Badge className="bg-[#2A9D8F]/10 text-[#2A9D8F] border-[#2A9D8F]/30">
              {sellerBenchmarkAverageLabel}
            </Badge>
            {sellerBenchmarkDeltaLabel && (
              <Badge className="bg-muted text-foreground border-border/60">
                {sellerBenchmarkDeltaLabel}
              </Badge>
            )}
          </CardContent>
        </Card>
      )}

      {/* Charts Row */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Sales Trend Chart */}
        <Card className="border-border/40" data-testid="sales-trend-chart">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Ventas del Mes Actual: Objetivo, Real y Pronóstico {isSellerView && '(Personal)'}
            </CardTitle>
            <p className="text-xs text-muted-foreground">{salesCalendarLabel}</p>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-64 w-full" />
            ) : (
              <>
                <div className="h-64">
                  <SafeChart resetKey={`${currentMonthKey}-${currentMonthTrendData.length}-sales`}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={currentMonthTrendData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                        <XAxis
                          dataKey={salesTrendXAxisKey}
                          tick={salesXAxisTick}
                          stroke="hsl(var(--muted-foreground))"
                          minTickGap={16}
                        />
                        <YAxis tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                        <Tooltip
                          contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '6px'
                          }}
                          formatter={(value, name) => [
                            formatUnits(value),
                            name
                          ]}
                        />
                        <Legend
                          payload={[
                            {
                              value: 'Año pasado (mismo mes)',
                              type: 'line',
                              id: 'legend-last-year',
                              color: '#6B7280'
                            },
                            {
                              value: objectiveLegendName,
                              type: 'line',
                              id: 'legend-weighted-objective',
                              color: '#E9C46A'
                            },
                            {
                              value: 'Pronóstico tendencia',
                              type: 'line',
                              id: 'legend-forecast',
                              color: '#2A9D8F'
                            },
                            {
                              value: 'Ventas reales',
                              type: 'line',
                              id: 'legend-real-sales',
                              color: '#002FA7'
                            }
                          ]}
                        />
                        {salesTrendXAxisKey === 'day_label' && fiscalCloseXAxisValue && (
                          <ReferenceLine
                            x={fiscalCloseXAxisValue}
                            stroke="#B45309"
                            strokeDasharray="4 4"
                            ifOverflow="extendDomain"
                            label={{ value: 'Cierre fiscal', position: 'insideTopLeft', fill: '#B45309', fontSize: 10 }}
                          />
                        )}
                        {salesTrendXAxisKey === 'day_label' && industryCloseXAxisValue && (
                          <ReferenceLine
                            x={industryCloseXAxisValue}
                            stroke="#0F766E"
                            strokeDasharray="4 4"
                            ifOverflow="extendDomain"
                            label={{ value: industryCloseXAxisLabel, position: 'insideTopRight', fill: '#0F766E', fontSize: 10 }}
                          />
                        )}
                        <Line
                          type="monotone"
                          dataKey="last_year_units"
                          stroke="#6B7280"
                          strokeWidth={2}
                          dot={{ fill: '#6B7280' }}
                          name="Año pasado (mismo mes)"
                        />
                        <Line
                          type="monotone"
                          dataKey="weighted_objective_units"
                          stroke="#E9C46A"
                          strokeWidth={2}
                          dot={{ fill: '#E9C46A' }}
                          name={objectiveLegendName}
                        />
                        <Line
                          type="monotone"
                          dataKey="units"
                          stroke="#002FA7"
                          strokeWidth={2}
                          dot={{ fill: '#002FA7' }}
                          name="Ventas reales"
                        />
                        <Line
                          type="monotone"
                          dataKey="forecast_units"
                          stroke="#2A9D8F"
                          strokeWidth={2}
                          strokeDasharray="6 4"
                          dot={{ fill: '#2A9D8F' }}
                          name="Pronóstico tendencia"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </SafeChart>
                </div>
                {!hasObjectiveData && (
                  <p className="text-xs text-muted-foreground mt-3">
                    No hay objetivos aprobados cargados en este alcance. La línea de objetivo ponderado se mostrará al registrar objetivos por agencia o vendedor.
                  </p>
                )}
                {hasObjectiveData && objectiveSource === 'benchmark_last_year' && (
                  <p className="text-xs text-muted-foreground mt-3">
                    Objetivo ponderado estimado con benchmark del mismo mes del año pasado (no hay objetivo aprobado vigente).
                  </p>
                )}
                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-4">
                  <div className="rounded-md border border-border/50 bg-muted/20 p-3">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Mes actual</div>
                    <div className="text-lg font-semibold">{formatUnits(currentMonthSummary.units)}</div>
                    <div className="text-xs text-muted-foreground">ventas reales</div>
                  </div>
                  <div className="rounded-md border border-border/50 bg-muted/20 p-3">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Objetivo mes actual</div>
                    <div className="text-lg font-semibold">{formatUnits(currentMonthSummary.objectiveUnits)}</div>
                    <div className="text-xs text-muted-foreground">objetivo ponderado</div>
                  </div>
                  <div className="rounded-md border border-border/50 bg-muted/20 p-3">
                    <div className="text-xs uppercase tracking-wide text-muted-foreground">Brecha vs objetivo</div>
                    <div className="text-lg font-semibold">{formatPercent(currentMonthSummary.objectiveGapPct)}</div>
                    <div className="text-xs text-muted-foreground">real contra objetivo</div>
                  </div>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        {/* Aging Distribution Chart (only for non-seller view) */}
        {!isSellerView ? (
          <Card className="border-border/40" data-testid="aging-distribution-chart">
            <CardHeader className="pb-2">
              <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
                Distribución de Aging
              </CardTitle>
            </CardHeader>
            <CardContent>
              {!hasBrandContext && (
                <p className="text-xs text-muted-foreground mb-3">
                  Mostrando todas las marcas del alcance seleccionado.
                </p>
              )}
              {loading ? (
                <Skeleton className="h-64 w-full" />
              ) : hasAgingData ? (
                <div className="h-64 flex items-center">
                  <SafeChart resetKey={`${currentMonthKey}-${agingGaussianData.length}-aging`}>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={agingGaussianData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                        <XAxis
                          type="number"
                          dataKey="aging_days"
                          tick={{ fontSize: 12 }}
                          stroke="hsl(var(--muted-foreground))"
                        />
                        <YAxis
                          tick={{ fontSize: 12 }}
                          stroke="hsl(var(--muted-foreground))"
                          allowDecimals={false}
                        />
                        <Tooltip
                          contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '6px'
                          }}
                          formatter={(value) => [Number(value || 0).toFixed(1), 'Campana Gauss (estimada)']}
                          labelFormatter={(label) => `${Number(label || 0).toFixed(0)} días`}
                        />
                        <Line
                          type="monotone"
                          dataKey="gauss_value"
                          stroke="#002FA7"
                          strokeWidth={3}
                          dot={false}
                          name="Campana de Aging"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </SafeChart>
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
              ) : (
                <div className="h-64 flex items-center justify-center text-center text-muted-foreground">
                  No hay inventario en stock para mostrar aging en este alcance.
                </div>
              )}
            </CardContent>
          </Card>
        ) : (
          /* Revenue Trend for Seller View */
          <Card className="border-border/40" data-testid="revenue-trend-chart">
            <CardHeader className="pb-2">
              <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
                Ingresos por Mes
              </CardTitle>
            </CardHeader>
            <CardContent>
              {loading ? (
                <Skeleton className="h-64 w-full" />
              ) : (
                <div className="h-64">
                  <SafeChart resetKey={`${currentMonthKey}-${trends.length}-revenue`}>
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={trends}>
                        <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                        <XAxis dataKey="month" tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                        <YAxis tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                        <Tooltip
                          contentStyle={{
                            backgroundColor: 'hsl(var(--card))',
                            border: '1px solid hsl(var(--border))',
                            borderRadius: '6px'
                          }}
                          formatter={(value) => formatCurrency(value)}
                        />
                        <Bar dataKey="revenue" fill="#002FA7" radius={[4, 4, 0, 0]} name="Ingresos" />
                      </BarChart>
                    </ResponsiveContainer>
                  </SafeChart>
                </div>
              )}
            </CardContent>
          </Card>
        )}
      </div>

      {/* Bottom Row */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Vehicle Detail (only for non-seller view when brand selected) */}
        {!isSellerView && hasBrandContext && (
          <Card className="border-border/40" data-testid="top-aging-vehicles">
            <CardHeader className="pb-2 flex flex-row items-center justify-between">
              <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
                Detalle Por Vehículo
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
                <div className="space-y-4">
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
                  </div>
                  <div className="rounded-md border border-border/40 overflow-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-muted/30">
                        <tr>
                          <th className="text-left font-semibold px-3 py-2">Vehículo</th>
                          <th className="text-left font-semibold px-3 py-2">Agencia</th>
                          <th className="text-right font-semibold px-3 py-2">Aging</th>
                          <th className="text-right font-semibold px-3 py-2">Costo Fin.</th>
                          <th className="text-right font-semibold px-3 py-2">Costo Vehículo</th>
                        </tr>
                      </thead>
                      <tbody>
                        {brandVehicles.slice(0, 15).map((vehicle) => (
                          <tr key={`detail-${vehicle.id}`} className="border-t border-border/40">
                            <td className="px-3 py-2">
                              <div className="font-medium">{vehicle.model} {vehicle.trim}</div>
                              <div className="text-xs text-muted-foreground">{vehicle.year} • {vehicle.color}</div>
                            </td>
                            <td className="px-3 py-2 text-muted-foreground">{vehicle.agency_name}</td>
                            <td className="px-3 py-2 text-right">{vehicle.aging_days} días</td>
                            <td className="px-3 py-2 text-right">{formatCurrency(vehicle.financial_cost || 0)}</td>
                            <td className="px-3 py-2 text-right">{formatCurrency(vehicle.purchase_price || 0)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {brandVehicles.length > 15 && (
                    <div className="text-xs text-muted-foreground">
                      Mostrando 15 de {brandVehicles.length} vehículos. Usa “Ver todos” para ver el inventario completo.
                    </div>
                  )}
                  {brandVehicles.length === 0 && (
                    <p className="text-center text-muted-foreground py-8">
                      No hay vehículos en inventario para esta marca.
                    </p>
                  )}
                  {topVehicles.length === 0 && brandVehicles.length > 0 && (
                    <div className="text-sm text-muted-foreground">
                      No hay vehículos con aging destacado.
                    </div>
                  )}
                  {topVehicles.length > 0 && (
                    <div className="text-xs text-muted-foreground">
                      Arriba: top por aging. Abajo: detalle por vehículo en la marca seleccionada.
                    </div>
                  )}
                  {topVehicles.length === 0 && brandVehicles.length === 0 && (
                    <div className="hidden" />
                  )}
                </div>
              )}
            </CardContent>
          </Card>
        )}

        {/* Smart Suggestions */}
        <Card
          className={`border-border/40 ${isSellerView || !hasBrandContext ? 'lg:col-span-2' : ''}`}
          data-testid="smart-suggestions"
        >
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Cabinet Grotesk' }}>
              <Lightning size={20} weight="duotone" className="text-[#E9C46A]" />
              {isSellerView ? 'Oportunidades de Venta' : 'Sugerencias Inteligentes'}
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3].map((i) => (
                  <Skeleton key={i} className="h-20 w-full" />
                ))}
              </div>
            ) : !hasBrandContext ? (
              <p className="text-center text-muted-foreground py-8">
                Selecciona una marca para activar Aging y sugerencias.
              </p>
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
                        Incentivo sugerido: {formatCurrency(suggestion.suggested_bonus)}
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
