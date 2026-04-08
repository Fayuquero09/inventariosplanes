import { useState, useEffect, useCallback } from 'react';
import { commissionRulesApi } from '../lib/api';
import { useHierarchicalFilters, HierarchicalFilters } from '../components/HierarchicalFilters';
import { Card, CardContent, CardHeader, CardTitle } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
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
import { Plus, Pencil, Trash, CurrencyDollar, Percent, Package, Coin } from '@phosphor-icons/react';
import { toast } from 'sonner';

const RULE_TYPES = [
  { value: 'per_unit', label: 'Por Unidad', icon: Package, description: 'Monto fijo por cada unidad vendida' },
  { value: 'percentage', label: 'Porcentaje', icon: Percent, description: 'Porcentaje del precio de venta' },
  { value: 'volume_bonus', label: 'Bono por Volumen', icon: CurrencyDollar, description: 'Bono al alcanzar cierto número de unidades' },
  { value: 'fi_bonus', label: 'Bono F&I', icon: Coin, description: 'Porcentaje sobre ingresos de F&I' }
];

export default function CommissionsPage() {
  const filters = useHierarchicalFilters();
  const { getFilterParams, selectedAgency } = filters;
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [formData, setFormData] = useState({
    agency_id: '',
    name: '',
    rule_type: 'per_unit',
    value: '',
    min_units: '',
    max_units: ''
  });

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const params = getFilterParams();
      const res = await commissionRulesApi.getAll(params);
      setRules(res.data);
    } catch (error) {
      toast.error('Error al cargar datos');
    } finally {
      setLoading(false);
    }
  }, [getFilterParams]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      const data = {
        ...formData,
        value: parseFloat(formData.value),
        min_units: formData.min_units ? parseInt(formData.min_units) : null,
        max_units: formData.max_units ? parseInt(formData.max_units) : null
      };

      if (editingRule) {
        await commissionRulesApi.update(editingRule.id, data);
        toast.success('Regla actualizada correctamente');
      } else {
        await commissionRulesApi.create(data);
        toast.success('Regla creada correctamente');
      }

      setIsDialogOpen(false);
      setEditingRule(null);
      resetForm();
      fetchData();
    } catch (error) {
      toast.error('Error al guardar regla');
    }
  };

  const resetForm = () => {
    setFormData({
      agency_id: selectedAgency !== 'all' ? selectedAgency : '',
      name: '',
      rule_type: 'per_unit',
      value: '',
      min_units: '',
      max_units: ''
    });
  };

  const handleEdit = (rule) => {
    setEditingRule(rule);
    setFormData({
      agency_id: rule.agency_id,
      name: rule.name,
      rule_type: rule.rule_type,
      value: rule.value.toString(),
      min_units: rule.min_units?.toString() || '',
      max_units: rule.max_units?.toString() || ''
    });
    setIsDialogOpen(true);
  };

  const handleDelete = async (id) => {
    if (!window.confirm('¿Estás seguro de eliminar esta regla?')) return;
    try {
      await commissionRulesApi.delete(id);
      toast.success('Regla eliminada');
      fetchData();
    } catch (error) {
      toast.error('Error al eliminar regla');
    }
  };

  const openNewDialog = () => {
    setEditingRule(null);
    resetForm();
    setIsDialogOpen(true);
  };

  const getRuleType = (type) => RULE_TYPES.find((t) => t.value === type);

  const formatValue = (rule) => {
    if (rule.rule_type === 'percentage' || rule.rule_type === 'fi_bonus') {
      return `${rule.value}%`;
    }
    return new Intl.NumberFormat('es-MX', {
      style: 'currency',
      currency: 'MXN',
      minimumFractionDigits: 0
    }).format(rule.value);
  };

  // Group rules by agency
  const rulesByAgency = rules.reduce((acc, rule) => {
    const key = rule.agency_id || 'general';
    if (!acc[key]) {
      acc[key] = {
        agency_name: rule.agency_name || 'General',
        brand_name: rule.brand_name,
        group_name: rule.group_name,
        rules: []
      };
    }
    acc[key].rules.push(rule);
    return acc;
  }, {});

  return (
    <div className="space-y-6" data-testid="commissions-page">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold" style={{ fontFamily: 'Cabinet Grotesk' }}>
            Reglas de Comisiones
          </h1>
          <p className="text-muted-foreground">
            Configura las reglas de comisión por agencia (aplican a todos los vendedores de la agencia)
          </p>
        </div>
        <Button 
          onClick={openNewDialog}
          className="bg-[#002FA7] hover:bg-[#002FA7]/90" 
          data-testid="add-rule-btn"
        >
          <Plus size={18} className="mr-2" />
          Nueva Regla
        </Button>
      </div>

      {/* Hierarchical Filters */}
      <HierarchicalFilters filters={filters} />

      {/* Rule Type Info */}
      <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {RULE_TYPES.map((type) => {
          const Icon = type.icon;
          return (
            <Card key={type.value} className="border-border/40">
              <CardContent className="p-4 flex items-start gap-3">
                <div className="w-10 h-10 rounded-md bg-[#002FA7]/10 flex items-center justify-center flex-shrink-0">
                  <Icon size={20} weight="duotone" className="text-[#002FA7]" />
                </div>
                <div>
                  <h3 className="font-medium text-sm">{type.label}</h3>
                  <p className="text-xs text-muted-foreground">{type.description}</p>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Rules by Agency */}
      {loading ? (
        <Card className="border-border/40">
          <CardContent className="p-4">
            <Skeleton className="h-64 w-full" />
          </CardContent>
        </Card>
      ) : Object.keys(rulesByAgency).length === 0 ? (
        <Card className="border-border/40">
          <CardContent className="text-center py-12">
            <CurrencyDollar size={48} className="mx-auto text-muted-foreground mb-4" />
            <p className="text-muted-foreground">No hay reglas de comisión configuradas</p>
            <p className="text-sm text-muted-foreground">Crea una regla para calcular comisiones automáticamente</p>
          </CardContent>
        </Card>
      ) : (
        Object.entries(rulesByAgency).map(([agencyId, data]) => (
          <Card key={agencyId} className="border-border/40">
            <CardHeader className="pb-2">
              <div className="flex items-center justify-between">
                <CardTitle className="text-lg">{data.agency_name}</CardTitle>
                <div className="text-sm text-muted-foreground">
                  {data.brand_name} • {data.group_name}
                </div>
              </div>
            </CardHeader>
            <div className="table-wrapper">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Nombre</TableHead>
                    <TableHead>Tipo</TableHead>
                    <TableHead className="text-right">Valor</TableHead>
                    <TableHead>Rango Unidades</TableHead>
                    <TableHead className="w-24"></TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.rules.map((rule) => {
                    const ruleType = getRuleType(rule.rule_type);
                    const Icon = ruleType?.icon || CurrencyDollar;
                    return (
                      <TableRow key={rule.id} data-testid={`rule-row-${rule.id}`}>
                        <TableCell className="font-medium">{rule.name}</TableCell>
                        <TableCell>
                          <Badge variant="outline" className="gap-1">
                            <Icon size={14} />
                            {ruleType?.label}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-right tabular-nums font-medium">
                          {formatValue(rule)}
                        </TableCell>
                        <TableCell>
                          {rule.min_units || rule.max_units ? (
                            <span className="text-sm">
                              {rule.min_units || 0} - {rule.max_units || '∞'}
                            </span>
                          ) : (
                            '-'
                          )}
                        </TableCell>
                        <TableCell>
                          <div className="flex justify-end gap-1">
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleEdit(rule)}
                              data-testid={`edit-rule-${rule.id}`}
                            >
                              <Pencil size={16} />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => handleDelete(rule.id)}
                              className="text-destructive hover:text-destructive"
                              data-testid={`delete-rule-${rule.id}`}
                            >
                              <Trash size={16} />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          </Card>
        ))
      )}

      {/* Dialog */}
      <Dialog open={isDialogOpen} onOpenChange={(open) => {
        setIsDialogOpen(open);
        if (!open) {
          setEditingRule(null);
          resetForm();
        }
      }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingRule ? 'Editar Regla' : 'Nueva Regla de Comisión'}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="agency_id">Agencia</Label>
              <Select
                value={formData.agency_id}
                onValueChange={(value) => setFormData({ ...formData, agency_id: value })}
                required
              >
                <SelectTrigger data-testid="rule-agency-select">
                  <SelectValue placeholder="Seleccionar agencia" />
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
              <Label htmlFor="name">Nombre de la Regla</Label>
              <Input
                id="name"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                placeholder="Ej: Comisión base por unidad"
                required
                data-testid="rule-name-input"
              />
            </div>
            <div className="space-y-2">
              <Label>Tipo de Regla</Label>
              <div className="grid grid-cols-2 gap-2">
                {RULE_TYPES.map((type) => {
                  const Icon = type.icon;
                  return (
                    <button
                      key={type.value}
                      type="button"
                      onClick={() => setFormData({ ...formData, rule_type: type.value })}
                      className={`p-3 rounded-md border text-left transition-fast ${
                        formData.rule_type === type.value
                          ? 'border-[#002FA7] bg-[#002FA7]/5'
                          : 'border-border hover:border-[#002FA7]/50'
                      }`}
                      data-testid={`rule-type-${type.value}`}
                    >
                      <Icon size={20} weight="duotone" className={formData.rule_type === type.value ? 'text-[#002FA7]' : 'text-muted-foreground'} />
                      <div className="font-medium text-sm mt-1">{type.label}</div>
                      <div className="text-xs text-muted-foreground">{type.description}</div>
                    </button>
                  );
                })}
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="value">
                {formData.rule_type === 'percentage' || formData.rule_type === 'fi_bonus' ? 'Porcentaje (%)' : 'Monto ($)'}
              </Label>
              <Input
                id="value"
                type="number"
                step="0.01"
                value={formData.value}
                onChange={(e) => setFormData({ ...formData, value: e.target.value })}
                placeholder={formData.rule_type === 'percentage' || formData.rule_type === 'fi_bonus' ? '2.5' : '5000'}
                required
                data-testid="rule-value-input"
              />
            </div>
            {formData.rule_type === 'volume_bonus' && (
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="min_units">Mínimo Unidades</Label>
                  <Input
                    id="min_units"
                    type="number"
                    value={formData.min_units}
                    onChange={(e) => setFormData({ ...formData, min_units: e.target.value })}
                    placeholder="5"
                    data-testid="rule-min-units-input"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="max_units">Máximo Unidades</Label>
                  <Input
                    id="max_units"
                    type="number"
                    value={formData.max_units}
                    onChange={(e) => setFormData({ ...formData, max_units: e.target.value })}
                    placeholder="10"
                    data-testid="rule-max-units-input"
                  />
                </div>
              </div>
            )}
            <div className="flex justify-end gap-2">
              <Button type="button" variant="outline" onClick={() => setIsDialogOpen(false)}>
                Cancelar
              </Button>
              <Button type="submit" className="bg-[#002FA7] hover:bg-[#002FA7]/90" data-testid="save-rule-btn">
                {editingRule ? 'Actualizar' : 'Crear'}
              </Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  );
}
