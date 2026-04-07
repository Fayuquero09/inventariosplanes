import { useState, useEffect, useCallback } from 'react';
import { financialRatesApi, groupsApi, brandsApi, agenciesApi } from '../lib/api';
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
import { Plus, Pencil, Trash, Percent, Calendar } from '@phosphor-icons/react';
import { toast } from 'sonner';

export default function FinancialRatesPage() {
  const [rates, setRates] = useState([]);
  const [groups, setGroups] = useState([]);
  const [brands, setBrands] = useState([]);
  const [agencies, setAgencies] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRate, setEditingRate] = useState(null);
  const [formData, setFormData] = useState({
    name: '',
    group_id: '',
    brand_id: '',
    agency_id: '',
    annual_rate: '',
    grace_days: '0'
  });

  const fetchData = useCallback(async () => {
    try {
      const [ratesRes, groupsRes, brandsRes, agenciesRes] = await Promise.all([
        financialRatesApi.getAll(),
        groupsApi.getAll(),
        brandsApi.getAll(),
        agenciesApi.getAll()
      ]);
      setRates(ratesRes.data);
      setGroups(groupsRes.data);
      setBrands(brandsRes.data);
      setAgencies(agenciesRes.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const data = {
        ...formData,
        annual_rate: parseFloat(formData.annual_rate),
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
      setFormData({
        name: '',
        group_id: '',
        brand_id: '',
        agency_id: '',
        annual_rate: '',
        grace_days: '0'
      });
      fetchData();
    } catch (error) {
      toast.error('Error al guardar tasa');
    }
  };

  const handleEdit = (rate) => {
    setEditingRate(rate);
    setFormData({
      name: rate.name,
      group_id: rate.group_id,
      brand_id: rate.brand_id || '',
      agency_id: rate.agency_id || '',
      annual_rate: rate.annual_rate.toString(),
      grace_days: rate.grace_days.toString()
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('¿Estás seguro de eliminar esta tasa?')) return;
    try {
      await financialRatesApi.delete(id);
      toast.success('Tasa eliminada');
      fetchData();
    } catch (error) {
      toast.error('Error al eliminar tasa');
    }
  };

  const getGroupName = (id) => groups.find((g) => g.id === id)?.name || '-';
  const getBrandName = (id) => brands.find((b) => b.id === id)?.name || '-';
  const getAgencyName = (id) => agencies.find((a) => a.id === id)?.name || '-';

  return (
    <div className="space-y-6" data-testid="financial-rates-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Tasas Financieras
          </h1>
          <p className="text-muted-foreground">
            Configura las tasas de costo financiero por grupo, marca o agencia
          </p>
        </div>
        <Dialog open={isDialogOpen} onOpenChange={(open) => {
          setIsDialogOpen(open);
          if (!open) {
            setEditingRate(null);
            setFormData({
              name: '',
              group_id: '',
              brand_id: '',
              agency_id: '',
              annual_rate: '',
              grace_days: '0'
            });
          }
        }}>
          <DialogTrigger asChild>
            <Button className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-rate-btn">
              <Plus size={18} className="mr-2" />
              Nueva Tasa
            </Button>
          </DialogTrigger>
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
                    {groups.map((group) => (
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
                  value={formData.brand_id}
                  onValueChange={(value) => setFormData({ ...formData, brand_id: value })}
                >
                  <SelectTrigger data-testid="rate-brand-select">
                    <SelectValue placeholder="Aplica a todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="">Aplica a todas</SelectItem>
                    {brands.filter((b) => b.group_id === formData.group_id).map((brand) => (
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
                  value={formData.agency_id}
                  onValueChange={(value) => setFormData({ ...formData, agency_id: value })}
                >
                  <SelectTrigger data-testid="rate-agency-select">
                    <SelectValue placeholder="Aplica a todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="">Aplica a todas</SelectItem>
                    {agencies.filter((a) => !formData.brand_id || a.brand_id === formData.brand_id).map((agency) => (
                      <SelectItem key={agency.id} value={agency.id}>
                        {agency.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="annual_rate">Tasa Anual (%)</Label>
                  <Input
                    id="annual_rate"
                    type="number"
                    step="0.01"
                    value={formData.annual_rate}
                    onChange={(e) => setFormData({ ...formData, annual_rate: e.target.value })}
                    placeholder="12.00"
                    required
                    data-testid="rate-annual-input"
                  />
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

      {/* Info Cards */}
      <div className="grid sm:grid-cols-2 gap-4">
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center flex-shrink-0">
              <Percent size={20} weight="duotone" className="text-[#002FA7]" />
            </div>
            <div>
              <h3 className="font-medium">Tasa Anual</h3>
              <p className="text-sm text-muted-foreground">
                La tasa se aplica diariamente sobre el precio de compra del vehículo para calcular el costo financiero.
              </p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#2A9D8F]/10 flex items-center justify-center flex-shrink-0">
              <Calendar size={20} weight="duotone" className="text-[#2A9D8F]" />
            </div>
            <div>
              <h3 className="font-medium">Días de Gracia</h3>
              <p className="text-sm text-muted-foreground">
                Período inicial sin generar costo financiero. El cálculo inicia después de los días de gracia.
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
                <TableHead className="text-right">Tasa Anual</TableHead>
                <TableHead className="text-right">Días de Gracia</TableHead>
                <TableHead className="w-24"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                [...Array(3)].map((_, i) => (
                  <TableRow key={i}>
                    {[...Array(7)].map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-5 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : rates.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center py-12">
                    <Percent size={48} className="mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No hay tasas configuradas</p>
                    <p className="text-sm text-muted-foreground">Crea una tasa para comenzar a calcular costos financieros</p>
                  </TableCell>
                </TableRow>
              ) : (
                rates.map((rate) => (
                  <TableRow key={rate.id} data-testid={`rate-row-${rate.id}`}>
                    <TableCell className="font-medium">{rate.name}</TableCell>
                    <TableCell>{getGroupName(rate.group_id)}</TableCell>
                    <TableCell>{rate.brand_id ? getBrandName(rate.brand_id) : 'Todas'}</TableCell>
                    <TableCell>{rate.agency_id ? getAgencyName(rate.agency_id) : 'Todas'}</TableCell>
                    <TableCell className="text-right tabular-nums font-medium">{rate.annual_rate}%</TableCell>
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
    </div>
  );
}
