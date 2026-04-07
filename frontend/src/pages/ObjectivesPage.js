import { useState, useEffect, useCallback } from 'react';
import { salesObjectivesApi, agenciesApi, dashboardApi } from '../lib/api';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Skeleton } from '../components/ui/skeleton';
import { Progress } from '../components/ui/progress';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger
} from '../components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '../components/ui/select';
import { Plus, Target, TrendUp, Users } from '@phosphor-icons/react';
import { toast } from 'sonner';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell
} from 'recharts';

const MONTHS = [
  { value: 1, label: 'Enero' },
  { value: 2, label: 'Febrero' },
  { value: 3, label: 'Marzo' },
  { value: 4, label: 'Abril' },
  { value: 5, label: 'Mayo' },
  { value: 6, label: 'Junio' },
  { value: 7, label: 'Julio' },
  { value: 8, label: 'Agosto' },
  { value: 9, label: 'Septiembre' },
  { value: 10, label: 'Octubre' },
  { value: 11, label: 'Noviembre' },
  { value: 12, label: 'Diciembre' }
];

export default function ObjectivesPage() {
  const [objectives, setObjectives] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [sellerPerformance, setSellerPerformance] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState(new Date().getMonth() + 1);
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [formData, setFormData] = useState({
    agency_id: '',
    month: new Date().getMonth() + 1,
    year: new Date().getFullYear(),
    units_target: '',
    revenue_target: '',
    vehicle_line: ''
  });

  const fetchData = useCallback(async () => {
    try {
      const [objectivesRes, agenciesRes, performanceRes] = await Promise.all([
        salesObjectivesApi.getAll({ month: selectedMonth, year: selectedYear }),
        agenciesApi.getAll(),
        dashboardApi.getSellerPerformance({ month: selectedMonth, year: selectedYear })
      ]);
      setObjectives(objectivesRes.data);
      setAgencies(agenciesRes.data);
      setSellerPerformance(performanceRes.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [selectedMonth, selectedYear]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await salesObjectivesApi.create({
        ...formData,
        units_target: parseInt(formData.units_target),
        revenue_target: parseFloat(formData.revenue_target),
        vehicle_line: formData.vehicle_line || null
      });
      toast.success('Objetivo creado correctamente');
      setIsDialogOpen(false);
      setFormData({
        agency_id: '',
        month: new Date().getMonth() + 1,
        year: new Date().getFullYear(),
        units_target: '',
        revenue_target: '',
        vehicle_line: ''
      });
      fetchData();
    } catch (error) {
      toast.error('Error al crear objetivo');
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(value);
  };

  const getProgressColor = (progress) => {
    if (progress >= 100) return '#2A9D8F';
    if (progress >= 75) return '#002FA7';
    if (progress >= 50) return '#E9C46A';
    return '#E63946';
  };

  const sellerChartData = sellerPerformance.map((s) => ({
    name: s.seller_name?.split(' ')[0] || 'Unknown',
    units: s.units,
    commission: s.commission
  }));

  return (
    <div className="space-y-6" data-testid="objectives-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Objetivos de Ventas
          </h1>
          <p className="text-muted-foreground">
            Gestiona objetivos y monitorea el desempeño de vendedores
          </p>
        </div>
        <div className="flex gap-2">
          <Select value={selectedMonth.toString()} onValueChange={(v) => setSelectedMonth(parseInt(v))}>
            <SelectTrigger className="w-[140px]" data-testid="select-month">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {MONTHS.map((m) => (
                <SelectItem key={m.value} value={m.value.toString()}>
                  {m.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select value={selectedYear.toString()} onValueChange={(v) => setSelectedYear(parseInt(v))}>
            <SelectTrigger className="w-[100px]" data-testid="select-year">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {[2024, 2025, 2026].map((y) => (
                <SelectItem key={y} value={y.toString()}>
                  {y}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
            <DialogTrigger asChild>
              <Button className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-objective-btn">
                <Plus size={18} className="mr-2" />
                Nuevo Objetivo
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Nuevo Objetivo de Ventas</DialogTitle>
              </DialogHeader>
              <form onSubmit={handleSubmit} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="agency_id">Agencia</Label>
                  <Select
                    value={formData.agency_id}
                    onValueChange={(value) => setFormData({ ...formData, agency_id: value })}
                    required
                  >
                    <SelectTrigger data-testid="objective-agency-select">
                      <SelectValue placeholder="Seleccionar agencia" />
                    </SelectTrigger>
                    <SelectContent>
                      {agencies.map((agency) => (
                        <SelectItem key={agency.id} value={agency.id}>
                          {agency.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="month">Mes</Label>
                    <Select
                      value={formData.month.toString()}
                      onValueChange={(value) => setFormData({ ...formData, month: parseInt(value) })}
                    >
                      <SelectTrigger data-testid="objective-month-select">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {MONTHS.map((m) => (
                          <SelectItem key={m.value} value={m.value.toString()}>
                            {m.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="year">Año</Label>
                    <Select
                      value={formData.year.toString()}
                      onValueChange={(value) => setFormData({ ...formData, year: parseInt(value) })}
                    >
                      <SelectTrigger data-testid="objective-year-select">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {[2024, 2025, 2026].map((y) => (
                          <SelectItem key={y} value={y.toString()}>
                            {y}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="units_target">Meta Unidades</Label>
                    <Input
                      id="units_target"
                      type="number"
                      value={formData.units_target}
                      onChange={(e) => setFormData({ ...formData, units_target: e.target.value })}
                      placeholder="50"
                      required
                      data-testid="objective-units-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="revenue_target">Meta Ingresos</Label>
                    <Input
                      id="revenue_target"
                      type="number"
                      value={formData.revenue_target}
                      onChange={(e) => setFormData({ ...formData, revenue_target: e.target.value })}
                      placeholder="5000000"
                      required
                      data-testid="objective-revenue-input"
                    />
                  </div>
                </div>
                <div className="space-y-2">
                  <Label htmlFor="vehicle_line">Línea de Vehículo (opcional)</Label>
                  <Input
                    id="vehicle_line"
                    value={formData.vehicle_line}
                    onChange={(e) => setFormData({ ...formData, vehicle_line: e.target.value })}
                    placeholder="Ej: SUV, Sedán, Pickup"
                    data-testid="objective-line-input"
                  />
                </div>
                <div className="flex justify-end gap-2">
                  <Button type="button" variant="outline" onClick={() => setIsDialogOpen(false)}>
                    Cancelar
                  </Button>
                  <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-objective-btn">
                    Crear
                  </Button>
                </div>
              </form>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      {/* Objectives Grid */}
      <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-4">
        {loading ? (
          [...Array(3)].map((_, i) => (
            <Card key={i} className="border-border/40">
              <CardContent className="p-4">
                <Skeleton className="h-32 w-full" />
              </CardContent>
            </Card>
          ))
        ) : objectives.length === 0 ? (
          <Card className="border-border/40 col-span-full">
            <CardContent className="p-12 text-center">
              <Target size={48} className="mx-auto text-muted-foreground mb-4" />
              <p className="text-muted-foreground">No hay objetivos para este período</p>
              <p className="text-sm text-muted-foreground">Crea un objetivo para comenzar</p>
            </CardContent>
          </Card>
        ) : (
          objectives.map((objective) => (
            <Card key={objective.id} className="border-border/40" data-testid={`objective-card-${objective.id}`}>
              <CardHeader className="pb-2">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base">{objective.agency_name}</CardTitle>
                  {objective.vehicle_line && (
                    <Badge variant="outline">{objective.vehicle_line}</Badge>
                  )}
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <div>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-muted-foreground">Unidades</span>
                    <span className="font-medium">{objective.units_sold} / {objective.units_target}</span>
                  </div>
                  <Progress
                    value={Math.min(objective.progress_units, 100)}
                    className="h-2"
                    style={{
                      '--progress-background': getProgressColor(objective.progress_units)
                    }}
                  />
                  <div className="text-right text-xs text-muted-foreground mt-1">
                    {objective.progress_units.toFixed(1)}%
                  </div>
                </div>
                <div>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-muted-foreground">Ingresos</span>
                    <span className="font-medium">{formatCurrency(objective.revenue_achieved)}</span>
                  </div>
                  <Progress
                    value={Math.min(objective.progress_revenue, 100)}
                    className="h-2"
                    style={{
                      '--progress-background': getProgressColor(objective.progress_revenue)
                    }}
                  />
                  <div className="flex justify-between text-xs text-muted-foreground mt-1">
                    <span>Meta: {formatCurrency(objective.revenue_target)}</span>
                    <span>{objective.progress_revenue.toFixed(1)}%</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))
        )}
      </div>

      {/* Seller Performance */}
      <div className="grid lg:grid-cols-2 gap-6">
        <Card className="border-border/40" data-testid="seller-performance-chart">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Cabinet Grotesk' }}>
              <Users size={20} weight="duotone" className="text-[#002FA7]" />
              Desempeño de Vendedores
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <Skeleton className="h-64 w-full" />
            ) : sellerPerformance.length === 0 ? (
              <div className="h-64 flex items-center justify-center text-muted-foreground">
                No hay datos de vendedores
              </div>
            ) : (
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={sellerChartData} layout="vertical">
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
                    <XAxis type="number" tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" />
                    <YAxis dataKey="name" type="category" tick={{ fontSize: 12 }} stroke="hsl(var(--muted-foreground))" width={80} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: 'hsl(var(--card))',
                        border: '1px solid hsl(var(--border))',
                        borderRadius: '6px'
                      }}
                    />
                    <Bar dataKey="units" fill="#002FA7" name="Unidades" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/40" data-testid="seller-performance-table">
          <CardHeader>
            <CardTitle className="text-lg" style={{ fontFamily: 'Cabinet Grotesk' }}>
              Comisiones del Mes
            </CardTitle>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="space-y-3">
                {[1, 2, 3, 4].map((i) => (
                  <Skeleton key={i} className="h-12 w-full" />
                ))}
              </div>
            ) : sellerPerformance.length === 0 ? (
              <div className="py-12 text-center text-muted-foreground">
                No hay datos de comisiones
              </div>
            ) : (
              <div className="space-y-2">
                {sellerPerformance.map((seller, index) => (
                  <div
                    key={seller.seller_id}
                    className="flex items-center justify-between p-3 rounded-md bg-muted/30"
                  >
                    <div className="flex items-center gap-3">
                      <div className="w-8 h-8 rounded-full bg-[#002FA7] flex items-center justify-center text-white text-sm font-medium">
                        {index + 1}
                      </div>
                      <div>
                        <div className="font-medium">{seller.seller_name}</div>
                        <div className="text-sm text-muted-foreground">{seller.units} unidades</div>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="font-medium text-[#2A9D8F]">{formatCurrency(seller.commission)}</div>
                      <div className="text-sm text-muted-foreground">{formatCurrency(seller.revenue)} ventas</div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
