import { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { financialRatesApi, groupsApi, brandsApi, agenciesApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Skeleton } from '../components/ui/skeleton';
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow
} from '../components/ui/table';
import { Plus, Pencil, Trash, Percent, Calendar, TrendUp } from '@phosphor-icons/react';
import { toast } from 'sonner';

// Tasa TIIE actual (esto podría venir de una API en el futuro)
const CURRENT_TIIE = 11.25;

export default function FinancialRatesPage() {
  const filters = useHierarchicalFilters();
  const { getFilterParams, selectedGroup } = filters;
  const [rates, setRates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRate, setEditingRate] = useState(null);
  const [formData, setFormData] = useState({
    name: '',
    group_id: '',
    brand_id: '',
    agency_id: '',
    tiie_rate: CURRENT_TIIE.toString(),
    spread: '',
    grace_days: '0'
  });

  const fetchRates = useCallback(async () => {
    setLoading(true);
    try {
      const params = getFilterParams();
      const res = await financialRatesApi.getAll(params);
      setRates(res.data);
    } catch (error) {
      toast.error('Error al cargar tasas');
    } finally {
      setLoading(false);
    }
  }, [getFilterParams]);

  useEffect(() => {
    fetchRates();
  }, [fetchRates]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const data = {
        ...formData,
        tiie_rate: parseFloat(formData.tiie_rate),
        spread: parseFloat(formData.spread),
        grace_days: parseInt(formData.grace_days),
        brand_id: formData.brand_id || null,
        agency_id: formData.agency_id || null
      };

      if (editingRate) {
        await financialRatesApi.update(editingRate.id, data);
        toast.success('Tasa actualizada correctamente');
      } else {
        await financialRatesApi.create(data);
        toast.success('Tasa creada correctamente');
      }

      setIsDialogOpen(false);
      setEditingRate(null);
      resetForm();
      fetchRates();
    } catch (error) {
      toast.error('Error al guardar tasa');
    }
  };

  const resetForm = () => {
    setFormData({
      name: '',
      group_id: selectedGroup !== 'all' ? selectedGroup : '',
      brand_id: '',
      agency_id: '',
      tiie_rate: CURRENT_TIIE.toString(),
      spread: '',
      grace_days: '0'
    });
  };

  const handleEdit = (rate) => {
    setEditingRate(rate);
    setFormData({
      name: rate.name,
      group_id: rate.group_id,
      brand_id: rate.brand_id || '',
      agency_id: rate.agency_id || '',
      tiie_rate: (rate.tiie_rate || CURRENT_TIIE).toString(),
      spread: (rate.spread || 0).toString(),
      grace_days: rate.grace_days.toString()
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('¿Estás seguro de eliminar esta tasa?')) return;
    try {
      await financialRatesApi.delete(id);
      toast.success('Tasa eliminada');
      fetchRates();
    } catch (error) {
      toast.error('Error al eliminar tasa');
    }
  };

  const openNewDialog = () => {
    setEditingRate(null);
    resetForm();
    setIsDialogOpen(true);
  };

  // Calculate total rate preview
  const previewTotalRate = () => {
    const tiie = parseFloat(formData.tiie_rate) || 0;
    const spread = parseFloat(formData.spread) || 0;
    return (tiie + spread).toFixed(2);
  };

  return (
    <div className="space-y-6" data-testid="financial-rates-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Tasas Financieras
          </h1>
          <p className="text-muted-foreground">
            Configura las tasas de costo financiero (TIIE + Spread) por grupo, marca o agencia
          </p>
        </div>
        <Button 
          onClick={openNewDialog}
          className="bg-[#002FA7] hover:bg-[#002FA7]/90" 
          data-testid="add-rate-btn"
        >
          <Plus size={18} className="mr-2" />
          Nueva Tasa
        </Button>
      </div>

      {/* Filters */}
      <HierarchicalFilters filters={filters} />

      {/* Info Cards */}
      <div className="grid sm:grid-cols-3 gap-4">
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center flex-shrink-0">
              <TrendUp size={20} weight="duotone" className="text-[#002FA7]" />
            </div>
            <div>
              <h3 className="font-medium">TIIE Actual</h3>
              <p className="text-2xl font-bold text-[#002FA7]">{CURRENT_TIIE}%</p>
              <p className="text-xs text-muted-foreground">Tasa Interbancaria de Equilibrio</p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#2A9D8F]/10 flex items-center justify-center flex-shrink-0">
              <Percent size={20} weight="duotone" className="text-[#2A9D8F]" />
            </div>
            <div>
              <h3 className="font-medium">Spread</h3>
              <p className="text-sm text-muted-foreground">
                Porcentaje adicional sobre TIIE configurable por grupo/marca/agencia
              </p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#E9C46A]/10 flex items-center justify-center flex-shrink-0">
              <Calendar size={20} weight="duotone" className="text-[#b89830]" />
            </div>
            <div>
              <h3 className="font-medium">Días de Gracia</h3>
              <p className="text-sm text-muted-foreground">
                Período inicial sin generar costo financiero
              </p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Table */}
      <Card className="border-border/40">
        <div className="table-wrapper">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Nombre</TableHead>
                <TableHead>Grupo</TableHead>
                <TableHead>Marca</TableHead>
                <TableHead>Agencia</TableHead>
                <TableHead className="text-right">TIIE</TableHead>
                <TableHead className="text-right">Spread</TableHead>
                <TableHead className="text-right">Tasa Total</TableHead>
                <TableHead className="text-right">Días Gracia</TableHead>
                <TableHead className="w-24"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                [...Array(3)].map((_, i) => (
                  <TableRow key={i}>
                    {[...Array(9)].map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-5 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : rates.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={9} className="text-center py-12">
                    <Percent size={48} className="mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No hay tasas configuradas</p>
                    <p className="text-sm text-muted-foreground">Crea una tasa para comenzar a calcular costos financieros</p>
                  </TableCell>
                </TableRow>
              ) : (
                rates.map((rate) => (
                  <TableRow key={rate.id} data-testid={`rate-row-${rate.id}`}>
                    <TableCell className="font-medium">{rate.name}</TableCell>
                    <TableCell>{rate.group_name || '-'}</TableCell>
                    <TableCell>{rate.brand_name || 'Todas'}</TableCell>
                    <TableCell>{rate.agency_name || 'Todas'}</TableCell>
                    <TableCell className="text-right tabular-nums">{rate.tiie_rate || CURRENT_TIIE}%</TableCell>
                    <TableCell className="text-right tabular-nums text-[#2A9D8F] font-medium">+{rate.spread || 0}%</TableCell>
                    <TableCell className="text-right tabular-nums font-bold text-[#002FA7]">{rate.total_rate?.toFixed(2) || (rate.tiie_rate + rate.spread).toFixed(2)}%</TableCell>
                    <TableCell className="text-right tabular-nums">{rate.grace_days}</TableCell>
                    <TableCell>
                      <div className="flex justify-end gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleEdit(rate)}
                          data-testid={`edit-rate-${rate.id}`}
                        >
                          <Pencil size={16} />
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleDelete(rate.id)}
                          className="text-destructive hover:text-destructive"
                          data-testid={`delete-rate-${rate.id}`}
                        >
                          <Trash size={16} />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </Card>

      {/* Dialog */}
      <Dialog open={isDialogOpen} onOpenChange={(open) => {
        setIsDialogOpen(open);
        if (!open) {
          setEditingRate(null);
          resetForm();
        }
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingRate ? 'Editar Tasa' : 'Nueva Tasa Financiera'}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="name">Nombre</Label>
              <Input
                id="name"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="Ej: Tasa General Toyota"
                required
                data-testid="rate-name-input"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="group_id">Grupo</Label>
              <Select
                value={formData.group_id}
                onValueChange={(value) => setFormData({ ...formData, group_id: value })}
                required
              >
                <SelectTrigger data-testid="rate-group-select">
                  <SelectValue placeholder="Seleccionar grupo" />
                </SelectTrigger>
                <SelectContent>
                  {filters.groups.map((group) => (
                    <SelectItem key={group.id} value={group.id}>
                      {group.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="brand_id">Marca (opcional)</Label>
              <Select
                value={formData.brand_id || 'none'}
                onValueChange={(value) => setFormData({ ...formData, brand_id: value === 'none' ? '' : value })}
              >
                <SelectTrigger data-testid="rate-brand-select">
                  <SelectValue placeholder="Aplica a todas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Aplica a todas</SelectItem>
                  {filters.brands.filter((b) => b.group_id === formData.group_id).map((brand) => (
                    <SelectItem key={brand.id} value={brand.id}>
                      {brand.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="agency_id">Agencia (opcional)</Label>
              <Select
                value={formData.agency_id || 'none'}
                onValueChange={(value) => setFormData({ ...formData, agency_id: value === 'none' ? '' : value })}
              >
                <SelectTrigger data-testid="rate-agency-select">
                  <SelectValue placeholder="Aplica a todas" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Aplica a todas</SelectItem>
                  {filters.agencies.filter((a) => !formData.brand_id || a.brand_id === formData.brand_id).map((agency) => (
                    <SelectItem key={agency.id} value={agency.id}>
                      {agency.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            {/* TIIE + Spread */}
            <div className="p-4 rounded-md bg-muted/50 space-y-4">
              <div className="grid grid-cols-3 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="tiie_rate">TIIE (%)</Label>
                  <Input
                    id="tiie_rate"
                    type="number"
                    step="0.01"
                    value={formData.tiie_rate}
                    onChange={(e) => setFormData({ ...formData, tiie_rate: e.target.value })}
                    required
                    data-testid="rate-tiie-input"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="spread">+ Spread (%)</Label>
                  <Input
                    id="spread"
                    type="number"
                    step="0.01"
                    value={formData.spread}
                    onChange={(e) => setFormData({ ...formData, spread: e.target.value })}
                    placeholder="2.5"
                    required
                    data-testid="rate-spread-input"
                  />
                </div>
                <div className="space-y-2">
                  <Label>= Tasa Total</Label>
                  <div className="h-10 px-3 py-2 rounded-md bg-[#002FA7]/10 flex items-center text-lg font-bold text-[#002FA7]">
                    {previewTotalRate()}%
                  </div>
                </div>
              </div>
            </div>
            
            <div className="space-y-2">
              <Label htmlFor="grace_days">Días de Gracia</Label>
              <Input
                id="grace_days"
                type="number"
                value={formData.grace_days}
                onChange={(e) => setFormData({ ...formData, grace_days: e.target.value })}
                placeholder="0"
                required
                data-testid="rate-grace-days-input"
              />
            </div>
            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => setIsDialogOpen(false)}>
                Cancelar
              </Button>
              <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-rate-btn">
                {editingRate ? 'Actualizar' : 'Crear'}
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
