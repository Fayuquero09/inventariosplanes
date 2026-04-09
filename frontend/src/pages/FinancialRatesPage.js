import { useState, useEffect, useCallback } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { financialRatesApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Skeleton } from '../components/ui/skeleton';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle
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

// Referencia TIIE para cálculo mensual (11.25% anual / 12).
const CURRENT_TIIE_ANNUAL = 11.25;
const CURRENT_TIIE = Number((CURRENT_TIIE_ANNUAL / 12).toFixed(4));
const RATE_MANAGER_ROLES = ['app_admin', 'group_finance_manager'];

export default function FinancialRatesPage() {
  const filters = useHierarchicalFilters();
  const {
    getFilterParams,
    selectedGroup,
    selectedBrand,
    selectedAgency,
    groups,
    brands,
    agencies
  } = filters;
  const { user } = useAuth();
  const canManageRates = RATE_MANAGER_ROLES.includes(user?.role);
  const [rates, setRates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRate, setEditingRate] = useState(null);
  const [isApplyingGroupDefault, setIsApplyingGroupDefault] = useState(false);
  const [formData, setFormData] = useState({
    name: '',
    group_id: '',
    brand_id: '',
    agency_id: '',
    tiie_rate: '',
    spread: '',
    grace_days: '0'
  });
  const [nameTouched, setNameTouched] = useState(false);
  const isGroupLevelScope = !formData.brand_id && !formData.agency_id;

  const parseOptionalRateInput = (value) => {
    const trimmed = String(value ?? '').trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : NaN;
  };

  const resolveScopeFromValues = (values) => {
    let groupId = values.group_id || '';
    let brandId = values.brand_id || '';
    let agencyId = values.agency_id || '';

    if (!groupId && agencyId) {
      const agency = agencies.find((item) => item.id === agencyId);
      if (agency?.group_id) groupId = agency.group_id;
      if (!brandId && agency?.brand_id) brandId = agency.brand_id;
    }
    if (!groupId && brandId) {
      const brand = brands.find((item) => item.id === brandId);
      if (brand?.group_id) groupId = brand.group_id;
    }

    return { group_id: groupId, brand_id: brandId, agency_id: agencyId };
  };

  const buildDefaultRateName = (scopeValues) => {
    const scope = resolveScopeFromValues(scopeValues);
    const groupName = groups.find((item) => item.id === scope.group_id)?.name || 'Grupo';
    const brandName = brands.find((item) => item.id === scope.brand_id)?.name;
    const agencyName = agencies.find((item) => item.id === scope.agency_id)?.name;

    if (agencyName) return `Tasa ${groupName} - ${agencyName}`;
    if (brandName) return `Tasa ${groupName} - ${brandName}`;
    return `Tasa General ${groupName}`;
  };

  const buildInitialForm = () => {
    const scope = resolveScopeFromValues({
      group_id: selectedGroup !== 'all' ? selectedGroup : (user?.group_id || ''),
      brand_id: selectedBrand !== 'all' ? selectedBrand : '',
      agency_id: selectedAgency !== 'all' ? selectedAgency : ''
    });
    return {
      name: buildDefaultRateName(scope),
      group_id: scope.group_id,
      brand_id: scope.brand_id,
      agency_id: scope.agency_id,
      tiie_rate: '',
      spread: '',
      grace_days: '0'
    };
  };

  const getLatestGroupBaseRate = (rateItems) => {
    const groupBaseRates = (Array.isArray(rateItems) ? rateItems : [])
      .filter((rate) => !rate.brand_id && !rate.agency_id)
      .sort((a, b) => {
        const timeA = new Date(a.created_at || 0).getTime();
        const timeB = new Date(b.created_at || 0).getTime();
        return timeB - timeA;
      });
    return groupBaseRates[0] || null;
  };

  const preloadGroupBaseRate = async (groupId) => {
    if (!groupId) return;
    try {
      const res = await financialRatesApi.getAll({ group_id: groupId });
      const baseRate = getLatestGroupBaseRate(res.data);
      if (!baseRate) return;
      setFormData((prev) => ({
        ...prev,
        tiie_rate: baseRate.tiie_rate == null ? prev.tiie_rate : Number(baseRate.tiie_rate).toString(),
        spread: baseRate.spread == null ? prev.spread : Number(baseRate.spread).toString(),
        grace_days: Number.isFinite(baseRate.grace_days) ? String(baseRate.grace_days) : prev.grace_days
      }));
    } catch {
      // Si falla la precarga, mantenemos el formulario editable sin bloquear al usuario.
    }
  };

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
    if (!canManageRates) {
      toast.error('No tienes permisos para modificar tasas');
      return;
    }
    try {
      const resolvedGroupId = formData.group_id || user?.group_id || '';
      if (!resolvedGroupId) {
        toast.error('Selecciona un grupo para la tasa');
        return;
      }

      const parsedTiie = parseOptionalRateInput(formData.tiie_rate);
      const parsedSpread = parseOptionalRateInput(formData.spread);
      const parsedGraceDays = parseInt(formData.grace_days, 10);

      if (!Number.isFinite(parsedGraceDays) || parsedGraceDays < 0) {
        toast.error('Días de gracia debe ser un número válido mayor o igual a 0');
        return;
      }

      if (isGroupLevelScope && (parsedTiie === null || parsedSpread === null)) {
        toast.error('La tasa general del grupo requiere TIIE mensual y Spread mensual');
        return;
      }
      if (parsedTiie !== null && !Number.isFinite(parsedTiie)) {
        toast.error('TIIE mensual inválida');
        return;
      }
      if (parsedSpread !== null && !Number.isFinite(parsedSpread)) {
        toast.error('Spread mensual inválido');
        return;
      }

      const data = {
        ...formData,
        name: String(formData.name || '').trim(),
        group_id: resolvedGroupId,
        tiie_rate: parsedTiie,
        spread: parsedSpread,
        grace_days: parsedGraceDays,
        brand_id: formData.brand_id || null,
        agency_id: formData.agency_id || null
      };
      if (!data.name) {
        data.name = buildDefaultRateName({
          group_id: data.group_id,
          brand_id: data.brand_id || '',
          agency_id: data.agency_id || ''
        });
      }

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
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al guardar tasa');
    }
  };

  const resetForm = () => {
    setNameTouched(false);
    setFormData(buildInitialForm());
  };

  const handleEdit = (rate) => {
    if (!canManageRates) {
      toast.error('No tienes permisos para editar tasas');
      return;
    }
    setEditingRate(rate);
    setNameTouched(true);
    setFormData({
      name: rate.name,
      group_id: rate.group_id,
      brand_id: rate.brand_id || '',
      agency_id: rate.agency_id || '',
      tiie_rate: rate.tiie_rate == null ? '' : Number(rate.tiie_rate).toString(),
      spread: rate.spread == null ? '' : Number(rate.spread).toString(),
      grace_days: rate.grace_days.toString()
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id) => {
    if (!canManageRates) {
      toast.error('No tienes permisos para eliminar tasas');
      return;
    }
    if (!window.confirm('¿Estás seguro de eliminar esta tasa?')) return;
    try {
      await financialRatesApi.delete(id);
      toast.success('Tasa eliminada');
      fetchRates();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al eliminar tasa');
    }
  };

  const openNewDialog = () => {
    if (!canManageRates) {
      toast.error('No tienes permisos para crear tasas');
      return;
    }
    setEditingRate(null);
    resetForm();
    setIsDialogOpen(true);
    const resolvedGroupId = getResolvedGroupId();
    if (resolvedGroupId) {
      preloadGroupBaseRate(resolvedGroupId);
    }
  };

  // Calculate total rate preview
  const previewTotalRate = () => {
    const tiie = parseOptionalRateInput(formData.tiie_rate);
    const spread = parseOptionalRateInput(formData.spread);
    if (!isGroupLevelScope && tiie === null && spread === null) return null;
    if (!Number.isFinite(tiie ?? 0) || !Number.isFinite(spread ?? 0)) return null;
    return ((tiie ?? 0) + (spread ?? 0)).toFixed(2);
  };

  const getResolvedGroupId = () => (
    selectedGroup !== 'all' ? selectedGroup : (user?.group_id || '')
  );

  const handleApplyGroupDefaultToBrands = async () => {
    if (!canManageRates) {
      toast.error('No tienes permisos para aplicar tasas masivas');
      return;
    }

    const groupId = getResolvedGroupId();
    if (!groupId) {
      toast.error('Selecciona un grupo para aplicar la tasa general');
      return;
    }

    const confirmed = window.confirm(
      'Se aplicará la tasa general a todas las marcas de este grupo que aún no tengan tasa propia. ¿Continuar?'
    );
    if (!confirmed) return;

    setIsApplyingGroupDefault(true);
    try {
      const res = await financialRatesApi.applyGroupDefault({ group_id: groupId });
      const created = Number(res?.data?.created_count || 0);
      const skipped = Number(res?.data?.skipped_count || 0);
      toast.success(`Aplicación completada: ${created} marcas creadas, ${skipped} ya tenían tasa.`);
      await fetchRates();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al aplicar tasa general a marcas');
    } finally {
      setIsApplyingGroupDefault(false);
    }
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
            Configura las tasas de costo financiero mensual (TIIE mensual + Spread mensual) por grupo, marca o agencia
          </p>
        </div>
        {canManageRates && (
          <div className="flex flex-wrap gap-2">
            <Button
              onClick={handleApplyGroupDefaultToBrands}
              variant="outline"
              disabled={!getResolvedGroupId() || isApplyingGroupDefault}
              data-testid="apply-group-default-rates-btn"
            >
              {isApplyingGroupDefault ? 'Aplicando...' : 'Aplicar tasa general a marcas'}
            </Button>
            <Button
              onClick={openNewDialog}
              className="bg-[#002FA7] hover:bg-[#002FA7]/90"
              data-testid="add-rate-btn"
            >
              <Plus size={18} className="mr-2" />
              Nueva Tasa
            </Button>
          </div>
        )}
      </div>
      {!canManageRates && (
        <p className="text-sm text-muted-foreground">
          Tu perfil tiene acceso de lectura en tasas financieras.
        </p>
      )}

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
              <h3 className="font-medium">TIIE Mensual</h3>
              <p className="text-2xl font-bold text-[#002FA7]">{CURRENT_TIIE}%</p>
              <p className="text-xs text-muted-foreground">Referencia mensual visual (no se aplica automáticamente)</p>
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/40">
          <CardContent className="p-4 flex items-start gap-4">
            <div className="w-10 h-10 rounded-md bg-[#2A9D8F]/10 flex items-center justify-center flex-shrink-0">
              <Percent size={20} weight="duotone" className="text-[#2A9D8F]" />
            </div>
            <div>
              <h3 className="font-medium">Spread Mensual</h3>
              <p className="text-sm text-muted-foreground">
                Porcentaje mensual adicional sobre TIIE configurable por grupo/marca/agencia
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
                <TableHead className="text-right">TIIE Mensual</TableHead>
                <TableHead className="text-right">Spread Mensual</TableHead>
                <TableHead className="text-right">Tasa Total Mensual</TableHead>
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
                    <TableCell className="text-right tabular-nums">
                      {rate.tiie_rate == null ? (
                        <span className="text-muted-foreground">
                          {rate.effective_tiie_rate == null
                            ? 'Hereda (sin tasa base)'
                            : `Hereda (${Number(rate.effective_tiie_rate).toFixed(2)}%)`}
                        </span>
                      ) : (
                        `${Number(rate.tiie_rate).toFixed(2)}%`
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-[#2A9D8F] font-medium">
                      {rate.spread == null ? (
                        <span className="text-muted-foreground">
                          {rate.effective_spread == null
                            ? 'Hereda (sin tasa base)'
                            : `Hereda (+${Number(rate.effective_spread).toFixed(2)}%)`}
                        </span>
                      ) : (
                        `+${Number(rate.spread).toFixed(2)}%`
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums font-bold text-[#002FA7]">
                      {rate.total_rate == null ? (
                        <span className="text-muted-foreground">
                          {rate.effective_total_rate == null
                            ? 'Hereda (sin tasa base)'
                            : `Hereda (${Number(rate.effective_total_rate).toFixed(2)}%)`}
                        </span>
                      ) : (
                        `${Number(rate.total_rate).toFixed(2)}%`
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{rate.grace_days}</TableCell>
                    <TableCell>
                      {canManageRates && (
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
                      )}
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </Card>

      {/* Dialog */}
      {canManageRates && (
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
                  onChange={(e) => {
                    setNameTouched(true);
                    setFormData({ ...formData, name: e.target.value });
                  }}
                  placeholder="Ej: Tasa General Toyota"
                  data-testid="rate-name-input"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="group_id">Grupo</Label>
                <Select
                  value={formData.group_id}
                  onValueChange={(value) => {
                    setFormData((prev) => {
                      const next = { ...prev, group_id: value, brand_id: '', agency_id: '' };
                      if (!nameTouched) {
                        next.name = buildDefaultRateName(next);
                      }
                      return next;
                    });
                    if (!editingRate) {
                      preloadGroupBaseRate(value);
                    }
                  }}
                  required
                  disabled={user?.role !== 'app_admin'}
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
                  onValueChange={(value) => {
                    setFormData((prev) => {
                      const next = { ...prev, brand_id: value === 'none' ? '' : value, agency_id: '' };
                      if (!nameTouched) {
                        next.name = buildDefaultRateName(next);
                      }
                      return next;
                    });
                  }}
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
                  onValueChange={(value) => {
                    setFormData((prev) => {
                      const next = { ...prev, agency_id: value === 'none' ? '' : value };
                      if (!nameTouched) {
                        next.name = buildDefaultRateName(next);
                      }
                      return next;
                    });
                  }}
                >
                  <SelectTrigger data-testid="rate-agency-select">
                    <SelectValue placeholder="Aplica a todas" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Aplica a todas</SelectItem>
                    {filters.agencies
                      .filter((a) => a.group_id === formData.group_id)
                      .filter((a) => !formData.brand_id || a.brand_id === formData.brand_id)
                      .map((agency) => (
                        <SelectItem key={agency.id} value={agency.id}>
                          {agency.name}
                        </SelectItem>
                      ))}
                  </SelectContent>
                </Select>
              </div>

              {/* TIIE mensual + Spread mensual */}
              <div className="p-4 rounded-md bg-muted/50 space-y-4">
                {!isGroupLevelScope && (
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                    <p className="text-xs text-muted-foreground">
                      En marca/agencia puedes definir tasa mensual propia (TIIE + Spread) o dejar ambos vacíos para heredar del nivel superior.
                    </p>
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={() => setFormData((prev) => ({ ...prev, tiie_rate: '', spread: '' }))}
                    >
                      Heredar tasa
                    </Button>
                  </div>
                )}
                <div className="grid grid-cols-3 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="tiie_rate">TIIE Mensual (%)</Label>
                    <Input
                      id="tiie_rate"
                      type="number"
                      step="0.01"
                      value={formData.tiie_rate}
                      onChange={(e) => setFormData({ ...formData, tiie_rate: e.target.value })}
                      required={isGroupLevelScope}
                      placeholder={!isGroupLevelScope ? 'Heredar' : undefined}
                      data-testid="rate-tiie-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="spread">+ Spread Mensual (%)</Label>
                    <Input
                      id="spread"
                      type="number"
                      step="0.01"
                      value={formData.spread}
                      onChange={(e) => setFormData({ ...formData, spread: e.target.value })}
                      placeholder={!isGroupLevelScope ? 'Heredar' : '0.17'}
                      required={isGroupLevelScope}
                      data-testid="rate-spread-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>= Tasa Total Mensual</Label>
                    <div className="h-10 px-3 py-2 rounded-md bg-[#002FA7]/10 flex items-center text-lg font-bold text-[#002FA7]">
                      {previewTotalRate() == null ? 'Heredada' : `${previewTotalRate()}%`}
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
      )}
    </div>
  );
}
