import { useState, useEffect, useCallback, useMemo } from 'react';
import { vehiclesApi, vehicleCatalogApi, dashboardApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { Card, CardContent } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
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
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger
} from '../components/ui/dropdown-menu';
import { Plus, Upload, MagnifyingGlass, DotsThree, Car } from '@phosphor-icons/react';
import { toast } from 'sonner';

function AgingBadge({ days }) {
  if (days <= 30) return <Badge className="aging-low">{days} días</Badge>;
  if (days <= 60) return <Badge className="aging-medium">{days} días</Badge>;
  return <Badge className="aging-high">{days} días</Badge>;
}

function StatusBadge({ status }) {
  const statusMap = {
    in_stock: { label: 'En Stock', className: 'status-in_stock' },
    sold: { label: 'Vendido', className: 'status-sold' },
    transferred: { label: 'Transferido', className: 'status-transferred' }
  };
  const config = statusMap[status] || statusMap.in_stock;
  return <Badge className={config.className}>{config.label}</Badge>;
}

export default function InventoryPage() {
  const filters = useHierarchicalFilters();
  const { getFilterParams } = filters;
  const [vehicles, setVehicles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState('all');
  const [filterType, setFilterType] = useState('all');
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });
  const [catalogYear, setCatalogYear] = useState(2026);
  const [catalogMakes, setCatalogMakes] = useState([]);
  const [catalogModels, setCatalogModels] = useState([]);
  const [catalogVersions, setCatalogVersions] = useState([]);
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [isAddDialogOpen, setIsAddDialogOpen] = useState(false);
  const [isImportDialogOpen, setIsImportDialogOpen] = useState(false);
  const [isAgingIncentiveDialogOpen, setIsAgingIncentiveDialogOpen] = useState(false);
  const [selectedVehicleForAgingIncentive, setSelectedVehicleForAgingIncentive] = useState(null);
  const [savingAgingIncentive, setSavingAgingIncentive] = useState(false);
  const [agingIncentiveForm, setAgingIncentiveForm] = useState({
    sale_discount_amount: '0',
    seller_bonus_amount: '0',
    notes: ''
  });
  const [newVehicle, setNewVehicle] = useState({
    vin: '',
    make: '',
    model: '',
    year: 2026,
    trim: '',
    color: '',
    vehicle_type: 'new',
    purchase_price: '',
    agency_id: ''
  });

  const getAgencyBrandName = useCallback((agencyId) => {
    if (!agencyId) return '';
    const agency = filters.agencies.find((item) => item.id === agencyId);
    if (!agency) return '';
    const brand = filters.brands.find((item) => item.id === agency.brand_id);
    return (brand?.name || '').trim();
  }, [filters.agencies, filters.brands]);

  const findCatalogMakeByName = useCallback((brandName) => {
    const normalized = (brandName || '').trim().toLowerCase();
    if (!normalized) return null;
    return catalogMakes.find((make) => (make.name || '').trim().toLowerCase() === normalized) || null;
  }, [catalogMakes]);

  const fetchCatalogMakes = useCallback(async () => {
    setCatalogLoading(true);
    try {
      const res = await vehicleCatalogApi.getMakes();
      const items = res.data?.items || [];
      setCatalogMakes(items);
      if (res.data?.model_year) {
        const year = parseInt(res.data.model_year, 10);
        if (!Number.isNaN(year)) {
          setCatalogYear(year);
          setNewVehicle((prev) => ({ ...prev, year }));
        }
      }
    } catch (error) {
      setCatalogMakes([]);
      toast.error('No se pudo cargar el catalogo de marcas/modelos');
    } finally {
      setCatalogLoading(false);
    }
  }, []);

  const fetchCatalogModels = useCallback(async (makeName) => {
    if (!makeName) {
      setCatalogModels([]);
      return;
    }
    try {
      const res = await vehicleCatalogApi.getModels(makeName);
      setCatalogModels(res.data?.items || []);
    } catch (error) {
      setCatalogModels([]);
      toast.error('No se pudieron cargar los modelos de la marca seleccionada');
    }
  }, []);

  const fetchCatalogVersions = useCallback(async (makeName, modelName) => {
    if (!makeName || !modelName) {
      setCatalogVersions([]);
      return;
    }
    try {
      const res = await vehicleCatalogApi.getVersions(makeName, modelName);
      setCatalogVersions(res.data?.items || []);
    } catch (error) {
      setCatalogVersions([]);
      toast.error('No se pudieron cargar las versiones del modelo seleccionado');
    }
  }, []);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = {
        ...getFilterParams(),
        status: filterStatus !== 'all' ? filterStatus : undefined,
        vehicle_type: filterType !== 'all' ? filterType : undefined,
        sold_current_month_only: true
      };
      const suggestionParams = {
        group_id: params.group_id,
        brand_id: params.brand_id,
        agency_id: params.agency_id,
        limit: 1000
      };
      const [vehiclesRes, suggestionsRes] = await Promise.all([
        vehiclesApi.getAll(params),
        dashboardApi.getSuggestions(suggestionParams)
      ]);
      const suggestionsMap = new Map(
        (suggestionsRes.data || []).map((item) => [item.vehicle_id, item])
      );
      const mergedVehicles = (vehiclesRes.data || []).map((vehicle) => {
        const suggestion = suggestionsMap.get(vehicle.id);
        return {
          ...vehicle,
          aging_suggested_bonus: Number(suggestion?.suggested_bonus || 0),
          aging_avg_days_to_sell: Number(suggestion?.avg_days_to_sell || 0),
          aging_suggestion_reason: suggestion?.reason || null
        };
      });
      setVehicles(mergedVehicles);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [getFilterParams, filterStatus, filterType]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    fetchCatalogMakes();
  }, [fetchCatalogMakes]);

  const resetVehicleForm = useCallback(() => {
    setNewVehicle({
      vin: '',
      make: '',
      model: '',
      year: catalogYear,
      trim: '',
      color: '',
      vehicle_type: 'new',
      purchase_price: '',
      agency_id: ''
    });
    setCatalogModels([]);
    setCatalogVersions([]);
  }, [catalogYear]);

  const handleAgencyChange = async (agencyId) => {
    const agencyBrandName = getAgencyBrandName(agencyId);
    const matchedMake = findCatalogMakeByName(agencyBrandName);
    const nextMake = matchedMake?.name || '';

    setNewVehicle((prev) => ({
      ...prev,
      agency_id: agencyId,
      make: nextMake,
      model: '',
      trim: '',
      year: catalogYear
    }));

    if (nextMake) {
      await fetchCatalogModels(nextMake);
    } else {
      setCatalogModels([]);
    }
    setCatalogVersions([]);
  };

  const handleMakeChange = async (makeName) => {
    setNewVehicle((prev) => ({
      ...prev,
      make: makeName,
      model: '',
      trim: '',
      year: catalogYear
    }));
    setCatalogVersions([]);
    await fetchCatalogModels(makeName);
  };

  const handleModelChange = async (modelName) => {
    setNewVehicle((prev) => ({
      ...prev,
      model: modelName,
      trim: '',
      year: catalogYear
    }));
    await fetchCatalogVersions(newVehicle.make, modelName);
  };

  const handleAddVehicle = async (e) => {
    e.preventDefault();
    if (!newVehicle.agency_id || !newVehicle.make || !newVehicle.model || !newVehicle.trim) {
      toast.error('Completa agencia, marca, modelo y version');
      return;
    }
    try {
      const { make, ...payload } = newVehicle;
      await vehiclesApi.create({
        ...payload,
        purchase_price: parseFloat(newVehicle.purchase_price),
        year: catalogYear
      });
      toast.success('Vehículo agregado correctamente');
      setIsAddDialogOpen(false);
      resetVehicleForm();
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al agregar vehículo');
    }
  };

  const handleImport = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      const result = await vehiclesApi.import(file);
      toast.success(`${result.data.imported} vehículos importados correctamente`);
      if (result.data.errors.length > 0) {
        toast.warning(`${result.data.errors.length} errores encontrados`);
      }
      setIsImportDialogOpen(false);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'Error al importar archivo');
    }
  };

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(value);
  };

  const formatSaleDate = (value) => {
    if (!value) return '—';
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return '—';
    return parsed.toLocaleDateString('es-MX', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric'
    });
  };

  const filteredVehicles = vehicles.filter((v) => {
    if (searchTerm) {
      const search = searchTerm.toLowerCase();
      return (
        v.vin?.toLowerCase().includes(search) ||
        v.model?.toLowerCase().includes(search) ||
        v.trim?.toLowerCase().includes(search) ||
        v.color?.toLowerCase().includes(search)
      );
    }
    return true;
  });

  const getSortValue = useCallback((vehicle, key) => {
    const grossProfit = vehicle.status === 'sold'
      ? Number(vehicle.sale_price || 0) - Number(vehicle.financial_cost || 0) - Number(vehicle.sale_commission || 0) - Number(vehicle.purchase_price || 0)
      : null;

    switch (key) {
      case 'vehicle':
        return `${vehicle.model || ''} ${vehicle.trim || ''}`.trim().toLowerCase();
      case 'vin':
        return (vehicle.vin || '').toLowerCase();
      case 'agency':
        return `${vehicle.agency_name || ''} ${vehicle.brand_name || ''}`.trim().toLowerCase();
      case 'type':
        return (vehicle.vehicle_type || '').toLowerCase();
      case 'price':
        return Number(vehicle.purchase_price || 0);
      case 'aging':
        return Number(vehicle.aging_days || 0);
      case 'financial_cost':
        return Number(vehicle.financial_cost || 0);
      case 'aging_incentive':
        return Number(vehicle.aging_suggested_bonus || 0);
      case 'sale_commission':
        return Number(vehicle.sale_commission || 0);
      case 'sale_price':
        return Number(vehicle.sale_price || 0);
      case 'sale_date':
        return vehicle.sale_date ? new Date(vehicle.sale_date).getTime() : 0;
      case 'gross_profit':
        return Number(grossProfit || 0);
      case 'status':
        return (vehicle.status || '').toLowerCase();
      default:
        return '';
    }
  }, []);

  const sortedVehicles = useMemo(() => {
    if (!sortConfig.key) return filteredVehicles;
    const items = [...filteredVehicles];
    items.sort((a, b) => {
      const left = getSortValue(a, sortConfig.key);
      const right = getSortValue(b, sortConfig.key);

      let comparison = 0;
      if (typeof left === 'number' && typeof right === 'number') {
        comparison = left - right;
      } else {
        comparison = String(left).localeCompare(String(right), 'es', { sensitivity: 'base' });
      }

      return sortConfig.direction === 'asc' ? comparison : -comparison;
    });
    return items;
  }, [filteredVehicles, sortConfig, getSortValue]);

  const toggleSort = (key) => {
    setSortConfig((prev) => {
      if (prev.key === key) {
        return {
          key,
          direction: prev.direction === 'asc' ? 'desc' : 'asc'
        };
      }
      return { key, direction: 'asc' };
    });
  };

  const renderSortIndicator = (key) => {
    if (sortConfig.key !== key) {
      return <span className="text-xs text-muted-foreground">↕</span>;
    }
    return <span className="text-xs text-[#002FA7]">{sortConfig.direction === 'asc' ? '↑' : '↓'}</span>;
  };

  const openAgingIncentiveDialog = (vehicle) => {
    const suggested = Number(vehicle?.aging_suggested_bonus || 0);
    setSelectedVehicleForAgingIncentive(vehicle);
    setAgingIncentiveForm({
      sale_discount_amount: String(Number(vehicle?.aging_incentive_plan?.sale_discount_amount || 0)),
      seller_bonus_amount: String(
        Number(
          vehicle?.aging_incentive_plan?.seller_bonus_amount
          ?? (suggested > 0 ? suggested : 0)
        )
      ),
      notes: vehicle?.aging_incentive_plan?.notes || ''
    });
    setIsAgingIncentiveDialogOpen(true);
  };

  const agingSuggestedAmount = Number(selectedVehicleForAgingIncentive?.aging_suggested_bonus || 0);
  const agingSaleDiscountAmount = Number(agingIncentiveForm.sale_discount_amount || 0);
  const agingSellerBonusAmount = Number(agingIncentiveForm.seller_bonus_amount || 0);
  const agingAppliedTotal = agingSaleDiscountAmount + agingSellerBonusAmount;
  const agingRemaining = agingSuggestedAmount - agingAppliedTotal;
  const agingExceedsSuggestion = agingAppliedTotal - agingSuggestedAmount > 0.01;

  const handleSaveAgingIncentive = async (e) => {
    e.preventDefault();
    if (!selectedVehicleForAgingIncentive?.id) return;
    if (agingAppliedTotal <= 0) {
      toast.error('Captura monto para venta o para vendedor');
      return;
    }
    if (agingSuggestedAmount <= 0) {
      toast.error('Este vehículo no tiene incentivo sugerido vigente');
      return;
    }
    if (agingExceedsSuggestion) {
      toast.error('El total aplicado no puede ser mayor al incentivo sugerido');
      return;
    }

    setSavingAgingIncentive(true);
    try {
      await vehiclesApi.applyAgingIncentive(selectedVehicleForAgingIncentive.id, {
        sale_discount_amount: agingSaleDiscountAmount,
        seller_bonus_amount: agingSellerBonusAmount,
        notes: agingIncentiveForm.notes?.trim() || null
      });
      toast.success('Incentivo de aging aplicado');
      setIsAgingIncentiveDialogOpen(false);
      setSelectedVehicleForAgingIncentive(null);
      fetchData();
    } catch (error) {
      const detail = error.response?.data?.detail;
      toast.error(typeof detail === 'string' ? detail : 'No se pudo aplicar el incentivo de aging');
    } finally {
      setSavingAgingIncentive(false);
    }
  };

  // Calculate totals
  const totals = {
    count: filteredVehicles.length,
    value: filteredVehicles.reduce((sum, v) => sum + (v.purchase_price || 0), 0),
    financialCost: filteredVehicles.reduce((sum, v) => sum + (v.financial_cost || 0), 0)
  };

  return (
    <div className="space-y-6" data-testid="inventory-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Inventario de Vehículos
          </h1>
          <p className="text-muted-foreground">
            {filteredVehicles.length} vehículos • {formatCurrency(totals.value)} valor • {formatCurrency(totals.financialCost)} costo financiero
          </p>
        </div>
        <div className="flex gap-2">
          <Dialog open={isImportDialogOpen} onOpenChange={setIsImportDialogOpen}>
            <DialogTrigger asChild>
              <Button variant="outline" data-testid="import-vehicles-btn">
                <Upload size={18} className="mr-2" />
                Importar
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Importar Vehículos</DialogTitle>
              </DialogHeader>
              <div className="space-y-4">
                <p className="text-sm text-muted-foreground">
                  Sube un archivo CSV o Excel con las columnas: vin, model, year, trim, color, vehicle_type, purchase_price, agency_id (solo año {catalogYear})
                </p>
                <Input
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={handleImport}
                  data-testid="import-file-input"
                />
              </div>
            </DialogContent>
          </Dialog>

          <Dialog
            open={isAddDialogOpen}
            onOpenChange={(open) => {
              setIsAddDialogOpen(open);
              if (open) resetVehicleForm();
            }}
          >
            <DialogTrigger asChild>
              <Button className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="add-vehicle-btn">
                <Plus size={18} className="mr-2" />
                Agregar Vehículo
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-[500px]">
              <DialogHeader>
                <DialogTitle>Agregar Vehículo</DialogTitle>
              </DialogHeader>
              <form onSubmit={handleAddVehicle} className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label htmlFor="vin">VIN</Label>
                    <Input
                      id="vin"
                      value={newVehicle.vin}
                      onChange={(e) => setNewVehicle({ ...newVehicle, vin: e.target.value })}
                      required
                      data-testid="vehicle-vin-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="agency_id">Agencia</Label>
                    <Select value={newVehicle.agency_id} onValueChange={handleAgencyChange}>
                      <SelectTrigger data-testid="vehicle-agency-select">
                        <SelectValue placeholder="Seleccionar" />
                      </SelectTrigger>
                      <SelectContent>
                        {filters.filteredAgencies.map((agency) => (
                          <SelectItem key={agency.id} value={agency.id}>
                            {agency.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Marca</Label>
                    <Select
                      value={newVehicle.make}
                      onValueChange={handleMakeChange}
                      disabled={catalogLoading || catalogMakes.length === 0}
                    >
                      <SelectTrigger data-testid="vehicle-make-select">
                        <SelectValue placeholder="Seleccionar marca" />
                      </SelectTrigger>
                      <SelectContent>
                        {catalogMakes.map((make) => (
                          <SelectItem key={make.name} value={make.name}>
                            {make.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Modelo</Label>
                    <Select
                      value={newVehicle.model}
                      onValueChange={handleModelChange}
                      disabled={!newVehicle.make || catalogModels.length === 0}
                    >
                      <SelectTrigger data-testid="vehicle-model-input">
                        <SelectValue placeholder="Seleccionar modelo" />
                      </SelectTrigger>
                      <SelectContent>
                        {catalogModels.map((model) => (
                          <SelectItem key={model.name} value={model.name}>
                            {model.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Versión</Label>
                    <Select
                      value={newVehicle.trim}
                      onValueChange={(value) => setNewVehicle((prev) => ({ ...prev, trim: value, year: catalogYear }))}
                      disabled={!newVehicle.model || catalogVersions.length === 0}
                    >
                      <SelectTrigger data-testid="vehicle-trim-input">
                        <SelectValue placeholder="Seleccionar versión" />
                      </SelectTrigger>
                      <SelectContent>
                        {catalogVersions.map((version) => (
                          <SelectItem key={version.name} value={version.name}>
                            {version.name}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="year">Año</Label>
                    <Input
                      id="year"
                      type="number"
                      value={catalogYear}
                      disabled
                      readOnly
                      data-testid="vehicle-year-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="color">Color</Label>
                    <Input
                      id="color"
                      value={newVehicle.color}
                      onChange={(e) => setNewVehicle({ ...newVehicle, color: e.target.value })}
                      required
                      data-testid="vehicle-color-input"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="vehicle_type">Tipo</Label>
                    <Select
                      value={newVehicle.vehicle_type}
                      onValueChange={(value) => setNewVehicle({ ...newVehicle, vehicle_type: value })}
                    >
                      <SelectTrigger data-testid="vehicle-type-select">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="new">Nuevo</SelectItem>
                        <SelectItem value="used">Seminuevo</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="purchase_price">Precio de Compra</Label>
                    <Input
                      id="purchase_price"
                      type="number"
                      value={newVehicle.purchase_price}
                      onChange={(e) => setNewVehicle({ ...newVehicle, purchase_price: e.target.value })}
                      required
                      data-testid="vehicle-price-input"
                    />
                  </div>
                </div>
                <div className="flex justify-end gap-2">
                  <Button type="button" variant="outline" onClick={() => setIsAddDialogOpen(false)}>
                    Cancelar
                  </Button>
                  <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-vehicle-btn">
                    Guardar
                  </Button>
                </div>
              </form>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <Dialog
        open={isAgingIncentiveDialogOpen}
        onOpenChange={(open) => {
          setIsAgingIncentiveDialogOpen(open);
          if (!open) setSelectedVehicleForAgingIncentive(null);
        }}
      >
        <DialogContent className="sm:max-w-[560px]">
          <DialogHeader>
            <DialogTitle>Aplicar Incentivo por Aging</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSaveAgingIncentive} className="space-y-4">
            <div className="rounded-md border border-border/60 bg-muted/30 p-3">
              <div className="font-medium">
                {selectedVehicleForAgingIncentive?.model} {selectedVehicleForAgingIncentive?.trim}
              </div>
              <div className="text-sm text-muted-foreground">
                VIN: {selectedVehicleForAgingIncentive?.vin} • Aging: {selectedVehicleForAgingIncentive?.aging_days || 0} días
              </div>
              <div className="text-sm mt-2">
                Sugerido: <span className="font-semibold">{formatCurrency(agingSuggestedAmount)}</span>
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant="outline"
                disabled={agingSuggestedAmount <= 0}
                onClick={() => setAgingIncentiveForm((prev) => ({
                  ...prev,
                  sale_discount_amount: String(agingSuggestedAmount),
                  seller_bonus_amount: '0'
                }))}
              >
                100% a Venta
              </Button>
              <Button
                type="button"
                variant="outline"
                disabled={agingSuggestedAmount <= 0}
                onClick={() => setAgingIncentiveForm((prev) => ({
                  ...prev,
                  sale_discount_amount: '0',
                  seller_bonus_amount: String(agingSuggestedAmount)
                }))}
              >
                100% a Vendedor
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={() => setAgingIncentiveForm((prev) => ({
                  ...prev,
                  sale_discount_amount: '0',
                  seller_bonus_amount: '0'
                }))}
              >
                Limpiar
              </Button>
            </div>

            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>Aplicar a venta ($)</Label>
                <Input
                  type="number"
                  min="0"
                  step="0.01"
                  value={agingIncentiveForm.sale_discount_amount}
                  onChange={(e) => setAgingIncentiveForm((prev) => ({ ...prev, sale_discount_amount: e.target.value }))}
                />
                <p className="text-xs text-muted-foreground">Este monto reduce precio de venta al registrar la operación.</p>
              </div>
              <div className="space-y-2">
                <Label>Aplicar a vendedor ($)</Label>
                <Input
                  type="number"
                  min="0"
                  step="0.01"
                  value={agingIncentiveForm.seller_bonus_amount}
                  onChange={(e) => setAgingIncentiveForm((prev) => ({ ...prev, seller_bonus_amount: e.target.value }))}
                />
                <p className="text-xs text-muted-foreground">Este monto se suma como bono directo a la comisión.</p>
              </div>
            </div>

            <div className="space-y-2">
              <Label>Notas (opcional)</Label>
              <Input
                value={agingIncentiveForm.notes}
                onChange={(e) => setAgingIncentiveForm((prev) => ({ ...prev, notes: e.target.value }))}
                placeholder="Ej: priorizar salida por costo financiero"
              />
            </div>

            <div className="rounded-md border border-border/60 p-3 text-sm">
              <div>Total aplicado: <span className="font-semibold">{formatCurrency(agingAppliedTotal)}</span></div>
              <div className={agingRemaining < 0 ? 'text-[#E63946]' : 'text-muted-foreground'}>
                Remanente sugerido: {formatCurrency(agingRemaining)}
              </div>
              {agingExceedsSuggestion && (
                <div className="text-[#E63946] mt-1">El total aplicado no puede superar el sugerido.</div>
              )}
            </div>

            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => setIsAgingIncentiveDialogOpen(false)}>
                Cancelar
              </Button>
              <Button
                type="submit"
                className="bg-[#002FA7] hover:bg-[#002FA7]/90"
                disabled={savingAgingIncentive || agingExceedsSuggestion || agingSuggestedAmount <= 0}
              >
                {savingAgingIncentive ? 'Guardando...' : 'Aplicar incentivo'}
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>

      {/* Hierarchical Filters */}
      <HierarchicalFilters filters={filters} />

      {/* Additional Filters */}
      <Card className="border-border/40">
        <CardContent className="p-4">
          <div className="flex flex-col sm:flex-row gap-4">
            <div className="flex-1 relative">
              <MagnifyingGlass size={18} className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Buscar por VIN, modelo, trim o color..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10"
                data-testid="search-vehicles-input"
              />
            </div>
            <Select value={filterStatus} onValueChange={setFilterStatus}>
              <SelectTrigger className="w-full sm:w-[150px]" data-testid="filter-status-select">
                <SelectValue placeholder="Todos los estados" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos los estados</SelectItem>
                <SelectItem value="in_stock">En Stock</SelectItem>
                <SelectItem value="sold">Vendido</SelectItem>
                <SelectItem value="transferred">Transferido</SelectItem>
              </SelectContent>
            </Select>
            <Select value={filterType} onValueChange={setFilterType}>
              <SelectTrigger className="w-full sm:w-[150px]" data-testid="filter-type-select">
                <SelectValue placeholder="Todos los tipos" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todos los tipos</SelectItem>
                <SelectItem value="new">Nuevos</SelectItem>
                <SelectItem value="used">Seminuevos</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card className="border-border/40">
        <div className="table-wrapper">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('vehicle')} className="flex items-center gap-1">
                    Vehículo {renderSortIndicator('vehicle')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('vin')} className="flex items-center gap-1">
                    VIN {renderSortIndicator('vin')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('agency')} className="flex items-center gap-1">
                    Agencia {renderSortIndicator('agency')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('type')} className="flex items-center gap-1">
                    Tipo {renderSortIndicator('type')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('price')} className="inline-flex items-center gap-1 justify-end w-full">
                    Precio {renderSortIndicator('price')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('aging')} className="flex items-center gap-1">
                    Aging {renderSortIndicator('aging')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('financial_cost')} className="inline-flex items-center gap-1 justify-end w-full">
                    Costo Fin. {renderSortIndicator('financial_cost')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('aging_incentive')} className="inline-flex items-center gap-1 justify-end w-full">
                    Incentivo Aging {renderSortIndicator('aging_incentive')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('sale_price')} className="inline-flex items-center gap-1 justify-end w-full">
                    Valor factura {renderSortIndicator('sale_price')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('sale_date')} className="flex items-center gap-1">
                    Fecha venta {renderSortIndicator('sale_date')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('sale_commission')} className="inline-flex items-center gap-1 justify-end w-full">
                    Comisión pagada {renderSortIndicator('sale_commission')}
                  </button>
                </TableHead>
                <TableHead className="text-right">
                  <button type="button" onClick={() => toggleSort('gross_profit')} className="inline-flex items-center gap-1 justify-end w-full">
                    Utilidad bruta {renderSortIndicator('gross_profit')}
                  </button>
                </TableHead>
                <TableHead>
                  <button type="button" onClick={() => toggleSort('status')} className="flex items-center gap-1">
                    Estado {renderSortIndicator('status')}
                  </button>
                </TableHead>
                <TableHead className="w-12"></TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                [...Array(5)].map((_, i) => (
                  <TableRow key={i}>
                    {[...Array(14)].map((_, j) => (
                      <TableCell key={j}>
                        <Skeleton className="h-5 w-full" />
                      </TableCell>
                    ))}
                  </TableRow>
                ))
              ) : sortedVehicles.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={14} className="text-center py-12">
                    <Car size={48} className="mx-auto text-muted-foreground mb-4" />
                    <p className="text-muted-foreground">No se encontraron vehículos</p>
                  </TableCell>
                </TableRow>
              ) : (
                sortedVehicles.map((vehicle) => (
                  <TableRow key={vehicle.id} data-testid={`vehicle-row-${vehicle.id}`}>
                    <TableCell>
                      <div className="font-medium">{vehicle.model} {vehicle.trim}</div>
                      <div className="text-sm text-muted-foreground">{vehicle.year} • {vehicle.color}</div>
                    </TableCell>
                    <TableCell className="font-mono text-sm">{vehicle.vin}</TableCell>
                    <TableCell>
                      <div>{vehicle.agency_name}</div>
                      <div className="text-sm text-muted-foreground">{vehicle.brand_name}</div>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {vehicle.vehicle_type === 'new' ? 'Nuevo' : 'Seminuevo'}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatCurrency(vehicle.purchase_price)}
                    </TableCell>
                    <TableCell>
                      <AgingBadge days={vehicle.aging_days} />
                    </TableCell>
                    <TableCell className="text-right tabular-nums text-[#E63946]">
                      {formatCurrency(vehicle.financial_cost)}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {vehicle.status !== 'in_stock' ? (
                        '—'
                      ) : (
                        <div>
                          {vehicle.aging_suggestion_reason ? (
                            <div>{formatCurrency(vehicle.aging_suggested_bonus)}</div>
                          ) : (
                            <div className="text-muted-foreground">Sin sugerencia</div>
                          )}
                          {vehicle.aging_incentive_plan?.active && (
                            <div className="text-xs text-muted-foreground mt-1">
                              Venta {formatCurrency(Number(vehicle.aging_incentive_plan.sale_discount_amount || 0))} • Vendedor {formatCurrency(Number(vehicle.aging_incentive_plan.seller_bonus_amount || 0))}
                            </div>
                          )}
                        </div>
                      )}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {vehicle.status === 'sold' && vehicle.sale_price !== undefined && vehicle.sale_price !== null
                        ? formatCurrency(vehicle.sale_price)
                        : '—'}
                    </TableCell>
                    <TableCell className="tabular-nums">
                      {vehicle.status === 'sold'
                        ? formatSaleDate(vehicle.sale_date)
                        : '—'}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {vehicle.status === 'sold' && vehicle.sale_commission !== undefined && vehicle.sale_commission !== null
                        ? formatCurrency(vehicle.sale_commission)
                        : '—'}
                    </TableCell>
                    <TableCell className={`text-right tabular-nums ${vehicle.status === 'sold' ? (Number(vehicle.sale_price || 0) - Number(vehicle.financial_cost || 0) - Number(vehicle.sale_commission || 0) - Number(vehicle.purchase_price || 0) >= 0 ? 'text-[#2A9D8F]' : 'text-[#E63946]') : ''}`}>
                      {vehicle.status === 'sold'
                        ? formatCurrency(Number(vehicle.sale_price || 0) - Number(vehicle.financial_cost || 0) - Number(vehicle.sale_commission || 0) - Number(vehicle.purchase_price || 0))
                        : '—'}
                    </TableCell>
                    <TableCell>
                      <StatusBadge status={vehicle.status} />
                    </TableCell>
                    <TableCell>
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button variant="ghost" size="sm">
                            <DotsThree size={20} />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end">
                          <DropdownMenuItem>Ver detalles</DropdownMenuItem>
                          <DropdownMenuItem>Editar</DropdownMenuItem>
                          <DropdownMenuItem>Registrar venta</DropdownMenuItem>
                          <DropdownMenuItem
                            onClick={() => openAgingIncentiveDialog(vehicle)}
                            disabled={vehicle.status !== 'in_stock' || (!vehicle.aging_suggestion_reason && !vehicle.aging_incentive_plan?.active)}
                          >
                            Aplicar incentivo aging
                          </DropdownMenuItem>
                        </DropdownMenuContent>
                      </DropdownMenu>
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
